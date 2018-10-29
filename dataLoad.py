import pandas as pd
from simpledbf import Dbf5
import os
import pandas_access as mdb
import dataDecode as dd
from time import gmtime, strftime


class dataLoad(object):
    def __init__(self, base_path, debug=False):
        self.debug = debug
        self.base_path = base_path
        self.defunciones = []
        self.lastCols = []
        self.decoder = dd.decoder(base_path + '_ref/', debug=self.debug)

    def load(self, test=False):
        self.load_test = test
        for file in os.listdir(self.base_path):
            name, ext = os.path.splitext(file)
            ext = ext.lower()
            self.loadfile(file)

    def col_compare(self, currentCols):
        result = ''
        for currentCol in currentCols:
            if not currentCol in self.lastCols:
                result += ' +' + currentCol
                self.lastCols.append(currentCol)
        for lastCol in self.lastCols:
            if not lastCol in currentCols:
                result += ' -' + lastCol
        return result

    def loadfile(self, file):
        ignored_files = ['.DS_Store', '_old', '_ref']
        if file in ignored_files:
            return

        name, ext = os.path.splitext(file)
        ext = ext.lower()
        supported_types = ['.dbf', '.csv', '.mdb', '.accdb', '.xlsx']
        if ext in supported_types:
            self.decoder.set_year(name)
            self.log('Reading: ' + file)
            continue_var = False
            try:
                load_function = getattr(self, 'load_' + ext[1:])
                continue_var = True
            except AttributeError:
                self.log('function not found: ' + 'load_' + ext[1:], True)
            if continue_var:
                cdf = load_function(file)
                self.log('Loaded {}: {}'.format(cdf.shape, file), True)
                cdf['origin'] = file
                # for key, value in self.decoder.get_categoricals().items():
                #     #print(key)
                #     cdf[key] = pd.Series(dtype=value)
                if self.load_test:
                    cdf = cdf.sample(n=self.load_test, random_state=31173).apply(self.decoder.decode___row, axis=1)
                    # remove absolute duplicates
                    # check for all nulls
                else:
                    cdf = cdf.apply(self.decoder.decode___row, axis=1)

                self.log('Decoded {}: {}'.format(cdf.shape, file), True)
                self.defunciones.append(cdf)
                self.log('columns comparison: {}'.format(self.col_compare(cdf.columns)))
        else:
            self.log(file + ' Not supported yet')

    def load_dbf(self, file):
        dbf = Dbf5(self.base_path + file)
        return dbf.to_dataframe()

    def load_csv(self, file):
        # TODO: check "NULL" values
        return pd.read_csv(self.base_path + file, sep=';', encoding='latin_1', low_memory=False)

    def load_mdb(self, file):
        name, ext = os.path.splitext(file)
        if name == '2007' or name == '2009':
            table = mdb.list_tables(self.base_path + file)[1]
        else:
            table = mdb.list_tables(self.base_path + file)[0]
        return mdb.read_table(self.base_path + file, table)

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
        return pd.concat(self.defunciones, sort=True)