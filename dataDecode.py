import numpy as np
import datetime, time, sys
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
        self.cache = {}
        self.dataframe_year = None
        self.load_maps()
        self.extra_cols = {}

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

        if self.debug:
            self.log('Creating pickles for: comunes, maps and categoricals')
            pk.dump(comunes, open('comunes.pk', 'wb'))
            pk.dump(self.maps, open("maps.pk", "wb"))
            pk.dump(self.categoricals, open("categoricals.pk", "wb"))

    def decodePass(self, key, data):
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
                    print('function not found: ' + 'def decode_' + key + '(self, key, column):')
                #decodeFunction = self.decodePass
                decodeFunction = self.decodeVoid

            try:
                name, data = decodeFunction(key, col)
            except TypeError as e:
                self.log('@decodeRow => decode function failed:')
                self.log('@decodeRow => {}'.format(sys.exc_info()[0]))
                self.log('@decoderow => {}'.format(e))
                self.log('@decodeRow => {}, {} = {}({}, {})'.format(name, data, decodeFunction, key, col))
                #self.log_invalid_type(key, col)
            if name:
                new_row[name] = data
                name, data = None, None
        self.cache = {}
        for col in self.extra_cols.keys():
            new_row[col] = self.extra_cols[col]
        return new_row

    def decode_dia_nac(self, key, column):
        try:
            self.cache['born_day'] = int(column)
        except:
            self.log('Invalid "born_day": {}, assingining 15'.format(column))
            self.cache['born_day'] = 15
            self.extra_cols['bird_date_accuracy'] = 'Month'
        return False, None

    def decode_mes_nac(self, key, column):
        try:
            self.cache['born_month'] = int(column)
        except:
            self.log('Invalid "born_month": {}, assingining 6'.format(column))
            self.cache['born_month'] = 6
            self.extra_cols['bird_date_accuracy'] = 'Year'
        return False, None

    def decode_ano1_nac(self, key, column):
        self.cache['born_century'] = int(column)
        return False, None

    def decode_ano2_nac(self, key, column):
        return self.decodeDate('bird_date', column, 'born_month', 'born_day', 'born_century')
        try:
            born_year = self.cache['born_century'] * 100 + int(column)
            bird_date = datetime.date(born_year, self.cache['born_month'], self.cache['born_day'])
        except:
            self.log('Invalid date: datetime.date({}*100 + {}, {}, {})'.format(self.cache['born_century'], column, column,self.cache['born_month'], self.cache['born_day']))
            bird_date = self.null
        return 'bird_date', bird_date

    def decodeDate(self, key, year, cached_month, cached_day, cached_century=None):
        if type(year) != str:
            if np.isnan(year):
                return key, None
        if cached_century:
            try:
                year = self.cache['born_century'] * 100 + int(year)
            except:
                self.log('Can\'t build a year from {}*100 + int({})'.format(self.cache['born_century'], year))
        else:
            year = int(year)
        try:
            date = datetime.date(year, self.cache[cached_month], self.cache[cached_day])
        except:
            self.log('Invalid date: datetime.date({}, {}, {})'.format(year,
                                                                 self.cache[cached_month],
                                                                 self.cache[cached_day]))
            date = None
        return key, date

    def decode_sexo(self, key, column):
        return self.decodeCategorical(key, column, 'assigned_sex', True, True)

    def decode_est_civil(self, key, column):
        return self.decodeCategorical(key, column, 'marital_status', True, True)

    def decode_edad_tipo(self, key, column):
        return self.decodeCategorical(key, column, 'age_type', True, True)

    def decode_peso(self, key, column):
        return self.decodeInt('born_weight_grams', column, ['9999',])

    def decode_gestacion(self, key, column):
        return self.decodeInt('gestation_age_weeks', column)

    def decodeInt(self, key, column, null_values=None):
        # null values init
        if type(column) != str:
            try:
                if np.isnan(column):
                    return key, None
            except TypeError as e:
                self.log('can\'t run np.isnan on: {}'.format(e))
        if not null_values:
            null_values = []
        if column in null_values:
            return key, None

        try:
            return key, int(column)
        except TypeError as e:
            self.log('Type error: {}'.format(e))
            self.log_invalid_type(key, column)
            return key, None

    def decode_edad_cant(self, key, column):
        return self.decodeInt('age_amount', column)

    def decode_curso_ins(self, key, column):
        return self.decodeInt('formal_education_years', column)

    def decode_nivel_ins(self, key, column):
        return self.decodeCategorical(key, column, 'formal_education_level', True, True)

    def decode_actividad(self, key, column):
        if type(column) != str:
            if np.isnan(column):
                return key, None
        try:
            self.cache['activity'] = int(column)
            return self.decodeCategorical(key, column, 'activity', True, True)
        except:
            self.log_invalid_type(key, column)
            return 'activity', None

    def decode_categoria(self, key, column):
        return self.decodeCategorical(key, column, 'occupational_category', True, True)

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
        # Remove full address (never should have been public)
        if self.dataframe_year == 2011:
            return self.decodeVoid()
        else:
            return self.decodeCategorical(key, column, 'decease_place', True, True)

    def decode_reg_res(self, key, column):
        return self.decodeCategorical(key, column, 'region', True,  True)

    def decode_serv_res(self, key, column):
        return self.decodeCategorical(key, column, 'health_service', True, True)

    def decode_comuna(self, key, column):
        if self.dataframe_year <= 1999:
            return self.decodeCategorical(key, column, 'comune', False, True, map_name='comunes_1812_1999')
        elif self.dataframe_year >= 2000 and self.dataframe_year <= 2007:
            return self.decodeCategorical(key, column, 'comune', False, True, map_name='comunes_2000_2007')
        elif self.dataframe_year >= 2008 and self.dataframe_year <= 2009:
            return self.decodeCategorical(key, column, 'comune', False, True, map_name='comunes_2008_2009')
        elif self.dataframe_year >= 2010 :
            return self.decodeCategorical(key, column, 'comune', False, True, map_name='comunes_2010_2018')

    def decode_urb_rural(self, key, column):
        return self.decodeCategorical(key, column, 'territory_class', True, True)

    def decode_diag1(self, key, column):
        code = column.lower()
        def remove_x(code):
            if(code[-1] == 'x'):
                code = code[0:-1]
        #print('{} {}'.format(column, type(column)))
        return self.decodeVoid()

    def decode_diag2(self, key, column):
        return self.decodeVoid()

    def decode_at_medica(self, key, column):
        return self.decodeCategorical(key, column, 'medical_attention', True, True)

    def decode_cal_medico(self, key, column):
        return self.decodeCategorical(key, column, 'reporter_role', True, True)

    def decode_cod_menor(self, key, column):
        return self.decodeCategorical(key, column, 'toddler', True, True)

    def decode_nutritivo(self, key, column):
        return self.decodeCategorical(key, column, 'nutrition_status', True, True)

    def decode_edad_m(self, key, column):
        return self.decodeInt('mother_age', column)

    def decode_est_civ_m(self, key, column):
        return self.decodeCategorical(key, column, 'mother_marital_status', True, True, 'marital_status', True)

    def decode_ocupacion(self, key, column):
        return self.decodeOcupation('ocupation', column, self.cache['activity'])
        if not 'activity' in self.cache:
            self.log('No activity to make a ocupation ({})'.format(column))
            return 'ocupation', None
        try:
            value = '{}.{}'.format(self.cache['activity'], column)
        except:
            self.log('Weird shit happening here :(')
            return 'ocupation', None
        try:
            return self.decodeCategorical(key, value, 'ocupation', cat_from_map=True)
        except TypeError as e:
            self.log(e)
            self.log_invalid_type(key, value)
            return 'ocupation', None

    def decodeOcupation(self, key, column, cached_activity):
        if cached_activity == None:
            return key, None
        try:
            value = '{}.{}'.format(cached_activity, column)
        except:
            self.log('Weird shit happening here :(')
            return key, None
        try:
            return self.decodeCategorical(key, value, key, map_name='ocupation',cat_from_map=True)
        except TypeError as e:
            self.log(e)
            self.log_invalid_type(key, value)
            return key, None


    def decode_hij_vivos(self, key, column):
        return self.decodeInt('mother_alive_childs', column)

    def decode_hij_fall(self, key, column):
        return self.decodeInt('mother_deaceased_childs', column)

    def decode_hij_mort(self, key, column):
        return self.decodeInt('mother_stillbirth_childs', column)

    def decode_hij_total(self, key, column):
        return self.decodeInt('mother_total_childs', column)

    def decode_parto_abor(self, key, column):
            return self.decodeCategorical(key, column, 'bird_abortion', True, True)

    def decode_dia_parto(self, key, column):
        try:
            self.cache['mother_last_bird_day'] = int(column)
        except:
            #self.log('Invalid "mother_last_bird_day": {}, assingining 15'.format(column))
            self.cache['mother_last_bird_day'] = 15
            self.extra_cols['mother_last_bird_accuracy'] = 'Month'
        return False, None

    def decode_mes_parto(self, key, column):
        try:
            self.cache['mother_last_bird_month'] = int(column)
        except:
            #self.log('Invalid "born_month": {}, assingining 6'.format(column))
            self.cache['mother_last_bird_month'] = 6
            self.extra_cols['mother_last_bird_accuracy'] = 'Year'
        return False, None

    def decode_ano_parto(self, key, column):
        if type(column) != str:
            if np.isnan(column):
                return key, None
        try:
            return 'mother_last_bird', datetime.date(int(column), self.cache['mother_last_bird_month'], self.cache['mother_last_bird_day'])
        except:
            self.log('Invalid value for "mother_last_bird": datetime.date({}, {}, {})'.format(column, self.cache['mother_last_bird_month'], self.cache['mother_last_bird_day']))
            return False, None

    def decode_activ_m(self, key, column):
        if type(column) != str:
            if np.isnan(column):
                return key, None
        try:
            self.cache['mother_activity'] = int(column)
            return self.decodeCategorical(key, column, 'mother_activity', True, True)
        except:
            self.log_invalid_type(key, column)
            return 'mother_activity', None

    def decode_ocupa_m(self, key, column):
        if not 'mother_activity' in self.cache:
            self.log('No activity to make a mother_occupation ({})'.format(column))
            return 'mother_occupation', None
        try:
            value = '{}.{}'.format(self.cache['mother_activity'], column)
        except:
            self.log('Weird shit happening here :(')
            return 'mother_occupation', None
        try:
            return self.decodeCategorical(key, value, 'mother_occupation', map_name='ocupation', cat_from_map=True)
        except TypeError as e:
            self.log(e)
            self.log_invalid_type(key, value)
            return 'mother_occupation', None

    def decode_origin(self, key, column):
        return self.decodePass(key, column)

    def decodeCategorical(self, key, value, name, key_make_string=False, key_make_int=False, map_name=None, cat_from_map=False):
        if type(value) != str:
            if np.isnan(value):
                return name, None
        if map_name is None:
            map_name = name
        if cat_from_map:
            cat_name = map_name
        else:
            cat_name = name
        try:
            if key_make_int:
                value = int(value)
            if key_make_string:
                value = str(value)
            return name,\
                pd.Categorical(self.maps[map_name].loc[value].value, dtype=self.categoricals[cat_name])
        except:
            self.log_invalid_type('{} ({}) '.format(name, key), value, map_name)
            return name, None

    def decodeVoid(self, key=None, column=None):
        return False, None

    def log(self, message, do_print=False):
        with open('dataDecode.log', 'a') as f:
            ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
            f.write("{} => ({}) {}\n".format(ts, self.dataframe_year, message))
        if do_print:
            print(message)

    def log_invalid_type(self, key, value, map_name=None):
        self.log('Invalid value for {}: "{}" {} (map: {})'.format(key, value, type(value), map_name))
