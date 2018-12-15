import pandas as pd
import dask.dataframe
from simpledbf import Dbf5
import os
import pandas_access as mdb
import dataDecode as dd
from time import gmtime, strftime
from tqdm.auto import tqdm
tqdm.pandas(unit='Rows')
import pickle as pk


class dataLoad(object):
    def __init__(self, base_path, debug=False, apply_meta=False, use_dask = False):
        self.debug = debug
        self.use_dask = use_dask
        self.dask_npartitions = 4
        self.base_path = base_path
        self.defunciones = []
        self.lastCols = []
        self.decoder = dd.decoder(base_path + '_ref/', debug=self.debug)
        self.sample_fraction = False
        self.total_rows_in = 0
        self.total_rows_out = 0
        self.null_values = ['NULL      ', 'NULL ']
        self.apply_meta = apply_meta

    def set_frac(self, frac):
        self.sample_fraction = frac

    def load(self, frac=False):
        self.sample_fraction = frac
        file_list = os.listdir(self.base_path)
        ignored_files = ['.floyddata', '.DS_Store', '_old', '_ref']
        for ignored_file in ignored_files:
            file_list.remove(ignored_file)
        # Iter trough files
        #print(file_list)
        #for file in tqdm(file_list, unit='file'):
        for file in file_list:
            name, ext = os.path.splitext(file)
            ext = ext.lower()
            self.loadfile(file)

    def col_compare(self, currentCols):
        result = ''
        for currentCol in currentCols:
            if not currentCol in self.lastCols:
                result += ' +' + currentCol
                #self.lastCols.append(currentCol)
        for lastCol in self.lastCols:
            if not lastCol in currentCols:
                result += ' -' + lastCol
        self.lastCols = currentCols
        return result

    # Executed for each file
    def loadfile(self, file, apply_meta=True):
        name, ext = os.path.splitext(file)
        ext = ext.lower()
        supported_types = ['.dbf', '.csv', '.mdb', '.accdb', '.xlsx']
        if ext in supported_types:
            self.decoder.set_year(name)
            self.log('Reading: ' + file)

            # Find a decoder for the file ext
            continue_var = False
            try:
                load_function = getattr(self, 'load_' + ext[1:])
                continue_var = True
            except AttributeError:
                self.log('function not found: ' + 'load_' + ext[1:], True)

            # Now with the decoder...
            if continue_var:
                # Data load
                cdf = load_function(file)
                self.log('Loaded ({},{}): {}'.format(len(cdf.columns), len(cdf), file), True)
                self.total_rows_in += len(cdf)
                cdf['origin'] = file

                # Build series of dtype wanted
                self.decoder.build_datetime(cdf)

                if self.use_dask:
                    cdf = dask.dataframe.from_pandas(cdf, npartitions=self.dask_npartitions)

                if self.sample_fraction:
                    #cdf = cdf.sample(frac=self.sample_fraction, random_state=31173)
                    cdf = cdf.sample(frac=self.sample_fraction)

                cdf = cdf.apply(self.decoder.decode___row, axis=1, meta=self.decoder.get_meta_raw())
                #cdf = cdf.apply(self.decoder.decode___row, axis=1)

                if self.use_dask:
                    cdf.compute()

                if self.apply_meta:
                    cdf = cdf.astype(self.decoder.get_meta())

                self.log('Decoded ({},{}): {}'.format(len(cdf.columns), len(cdf), file), True)
                self.total_rows_out += len(cdf)
                self.defunciones.append(cdf)
                self.log('columns comparison: {}'.format(self.col_compare(cdf.columns)))
        else:
            self.log(file + ' Not supported yet')

    def load_dbf(self, file):
        dbf = Dbf5(self.base_path + file)
        return dbf.to_dataframe(na='none')

    def load_csv(self, file):
        # TODO: check "NULL" values
        df = pd.read_csv(self.base_path + file, sep=';', encoding='latin_1', low_memory=False, na_values=self.null_values)

        # extra rows at 2011
        if file == '2011.csv':
            df = df.drop(columns=['LOCA_DEF', 'LUGAR_DEF', 'C_MEDICO', 'MV_CIRCUNT', 'MV_TIPO', 'MV_LUGAR'])
        return df

    def load_mdb(self, file):
        name, ext = os.path.splitext(file)
        if name == '2007' or name == '2009':
            table = mdb.list_tables(self.base_path + file)[1]
        else:
            table = mdb.list_tables(self.base_path + file)[0]

        df = mdb.read_table(self.base_path + file, table)

        # extra rows at 2012
        if file == '2012.accdb':
            df = df.drop(columns=['MV_CIRCUNT', 'MV_TIPO', 'C_MEDICO'])

        return df

    def load_accdb(self, file):
        return self.load_mdb(file)

    def load_xlsx(self, file):
        return pd.read_excel(self.base_path + file)

    def log(self, message, do_print=False):
        with open('dataLoad.log', 'a') as f:
            f.write("{}: {}\n".format(strftime("%Y-%m-%d %H:%M:%S", gmtime()), message))
        if do_print:
            print(message)

    def get_result(self):
        self.log('Total in: {}'.format(self.total_rows_in), True)
        self.log('Total out: {}'.format(self.total_rows_out), True)
        if self.use_dask:
            return dask.dataframe.concat(self.defunciones, interleave_partitions=True)
        else:
            #pk.dump(self.decoder.get_meta(), open('meta.pk', 'wb'))
            return pd.concat(self.defunciones, sort=False) #.astype(self.decoder.get_meta())