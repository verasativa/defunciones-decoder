import numpy as np
import datetime
import pandas as pd
import pickle as pk


class decoder(object):
    def __init__(self, ref_path, null_type=np.nan, debug=False):
        self.base_path = ref_path
        self.debug = debug
        self.null = null_type
        self.funcnonfund = []
        self.maps = {}
        self.categoricals = {}
        self.load_maps()
        self.cache = {}

    def get_categoricals(self):
        return self.categoricals

    def set_year(self, year):
        self.dataframe_year = int(year)

    def load_maps(self):
        self.load_maps_general()
        self.load_maps_comunes()


    def load_maps_general(self):
        col_codes = pd.read_csv(self.base_path + 'columns_codes.csv', true_values='TRUE', false_values=['FALSE'])
        for type in col_codes.type.unique():
            self.maps[type] = col_codes[col_codes.type == type].set_index('code').drop('type', axis=1)
            self.categoricals[type] = pd.api.types.CategoricalDtype(self.maps[type].value.dropna().values)

    def load_maps_comunes(self):
        comunes = pd.read_excel(
            self.base_path + 'División-Político-Administrativa-y-Servicios-de-Salud-Histórico.xls',
            na_values='Ignorada')
        comunes.drop(columns=comunes.columns[5:], inplace=True)
        comunes.rename(columns={'Nombre Comuna': 'value'}, inplace=True)

        self.maps['comunes_1812_1999'] = comunes\
            .rename(columns={'Código Comuna hasta 1999': 'code'})\
            .set_index('code')\
            .drop(columns=comunes.columns[1:4])

        self.maps['comunes_2000_2007'] = comunes\
            .rename(columns={'Código Comuna desde 2000': 'code'})\
            .set_index('code')\
            .drop(columns=comunes.columns[[0,2,3]])

        self.maps['comunes_2008_2009'] = comunes\
            .rename(columns={'Código Comuna desde 2008': 'code'})\
            .set_index('code')\
            .drop(columns=comunes.columns[[0, 1, 3]])

        self.maps['comunes_2010_2018'] = comunes\
            .rename(columns={'Código Comuna desde 2010': 'code'})\
            .set_index('code')\
            .drop(columns=comunes.columns[[0, 1, 2]])


        self.categoricals['comune'] = pd.api.types.CategoricalDtype(comunes.dropna().value.values)
        pk.dump(comunes, open('comunes.pk', 'wb'))
        pk.dump(self.maps, open("maps.pk", "wb"))
        pk.dump(self.categoricals, open("categoricals.pk", "wb"))

    def checkNan(self, data):
        if np.isnan(data):
            return np.nan

    def decode_default(self, key, data):
        return key, data

    def decodeRow(self, row):
        new_row = pd.Series()
        for key,col in row.iteritems():
            key = key.lower()
            try:
                decodeFunction = getattr(self, 'decode_' + key)
            except AttributeError:
                if not 'decode_' + key in self.funcnonfund:
                    self.funcnonfund.append('decode_' + key)
                    print('function not found: ' + 'decode_' + key)
                decodeFunction = self.decode_default
                #decodeFunction = self.decode_void

            try:
                name, data = decodeFunction(key, col)
            except:
                self.log_invalid_type(key, col)
            if name:
                new_row[name] = data
        self.cache = {}
        return new_row

    def decode_dia_nac(self, key, column):
        try:
            self.cache['born_day'] = int(column)
        except:
            self.log('Invalid "born_day": {}, assingining 15'.format(column))
            self.cache['born_day'] = 15
        return False, None

    def decode_mes_nac(self, key, column):
        self.cache['born_month'] = int(column)
        return False, None

    def decode_ano1_nac(self, key, column):
        self.cache['born_century'] = int(column)
        return False, None

    def decode_ano2_nac(self, key, column):
        try:
            born_year = self.cache['born_century'] * 100 + int(column)
            bird_date = datetime.date(born_year, self.cache['born_month'], self.cache['born_day'])
        except:
            self.log('Invalid date: datetime.date({}*100 + {}, {}, {})'.format(self.cache['born_century'], column, column,self.cache['born_month'], self.cache['born_day']))
            bird_date = self.null
        return 'bird_date', bird_date

    def decode_sexo(self, key, column):
        return self.decodeCategorical(key, int(column), 'assigned_sex')

    def decode_est_civil(self, key, column):
        return self.decodeCategorical(key, int(column), 'marital_status')

    def decode_edad_tipo(self, key, column):
        return self.decodeCategorical(key, int(column), 'age_type')

    def decode_edad_cant(self, key, column):
        try:
            return 'age_value', int(column)
        except:
            self.log('Invalid value for "age_value": ' + column)
            return 'age_value', None

    def decode_curso_ins(self, key, column):
        try:
            return 'formal_education_years', int(column)
        except:
            self.log('Invalid value for "formal_education_years": ' + column)
            return 'formal_education_years', None

    def decode_nivel_ins(self, key, column):
        return self.decodeCategorical(key, int(column), 'formal_education_level')

    def decode_actividad(self, key, column):
        try:
            self.cache['activity'] = int(column)
            return 'activity',\
                   pd.Categorical(self.maps['activity'].loc[str(int(column))], dtype=self.categoricals['activity'])
        except:
            self.log('Invalid value for "activity": ' + column)
            return 'activity', None

    def decode_ocupacion(self, key, column):
        try:
            ocupation = '{}.{}'.format(self.cache['activity'], column)
            return 'ocupation',\
                   pd.Categorical(self.maps['ocupation'].loc[ocupation], dtype=self.categoricals['ocupation'])
        except:
            self.log_invalid_type(key, column)
            return 'ocupation', None

    def decode_categoria(self, key, column):
        return self.decodeCategorical(key, int(column), 'occupational_category')

    def decode_dia_def(self, key, column):
        try:
            self.cache['deacease_day'] = int(column)
            return False, None
        except:
            self.log_invalid_type(key, column)
            return False, None

    def decode_mes_def(self, key, column):
        try:
            self.cache['deacease_month'] = int(column)
            return False, None
        except:
            self.log_invalid_type(key, column)
            return False, None

    def decode_ano_def(self, key, column):
        try:
            return 'deacease_date', datetime.date(int(column), self.cache['deacease_month'], self.cache['deacease_day'])
        except:
            self.log('Invalid value for "deacease_date": datetime.date({}, {}, {})'.format(int(column), self.cache['deacease_month'], self.cache['deacease_day']))
            return False, None

    def decode_lugar_def(self, key, column):
        try:
            column = int(column)
            return self.decodeCategorical(key, column, 'decease_place')
        except:
            self.log_invalid_type(key, column, 'decease_place')

    def decode_reg_res(self, key, column):
        return self.decodeCategorical(key, int(column), 'region')

    def decode_serv_res(self, key, column):
        return self.decodeCategorical(key, int(column), 'health_service')

    def decode_comuna(self, key, column):
        if self.dataframe_year <= 1999:
            return self.decodeCategorical(key, int(column), 'comune', key_make_string=True, map_name='comunes_1812_1999')
        elif self.dataframe_year >= 2000 and self.dataframe_year <= 2007:
            return self.decodeCategorical(key, int(column), 'comune', key_make_string=True, map_name='comunes_2000_2007')
        elif self.dataframe_year >= 2008 and self.dataframe_year <= 2009:
            return self.decodeCategorical(key, int(column), 'comune', key_make_string=True, map_name='comunes_2008_2009')
        elif self.dataframe_year >= 2010 :
            return self.decodeCategorical(key, int(column), 'comune', key_make_string=True, map_name='comunes_2010_2018')

    def decode_urb_rural(self, key, column):
        return self.decodeCategorical(key, int(column), 'territory_class')

    def decode_diag1(self, key, column):
        code = column.lower()
        def remove_x(code):
            if(code[-1] == 'x'):
                code = code[0:-1]
        print('{} {}'.format(column, type(column)))
        return False, None

    def decodeCategorical(self, key, value, name, key_make_string=False, map_name=None):
        if map_name is None:
            map_name = name
        try:
            if not key_make_string:
                value = str(value)
            return name,\
                pd.Categorical(self.maps[map_name].loc[value], dtype=self.categoricals[name])
        except:
            self.log_invalid_type(key, value, map_name)
            return name, None

    def decode_void(self, key, column):
        return False, None

    def log(self, message):
        with open('dataDecode.log', 'w') as f:
            f.write(message)
        print(message)

    def log_invalid_type(self, key, value, map_name=None):
        self.log('Invalid value for {}: "{}" {} (map: {})'.format(key, value, type(value), map_name))