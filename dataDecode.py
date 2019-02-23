import numpy as np
import datetime, time, sys
import pandas as pd
import pickle as pk
from pandas.api.types import CategoricalDtype


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
        # Inicialize always
        self.prepend_cols = {
            'comune':           None,
            'diagnosis_source': None,
            # Missing on 2016
            'toddler':          None,
            # Missing on 2001
            'mother_ocupation': None,
            'father_ocupation': None,
        }
        # add if not precent
        self.append_cols_base = {
            'bird_date_accuracy':        'day',
            'mother_last_bird_accuracy': 'day',
            'deacease_date_accuracy': 'day',
        }
        self.append_cols = self.append_cols_base.copy()
        # Ignored to decode
        self.pre_build_cols = [
            'bird_date',
            'deacease_date',
            'mother_last_bird',
        ]

    def get_categoricals(self):
        return self.categoricals

    def set_year(self, year):
        self.dataframe_year = int(float(year))

    def load_maps(self):
        self.load_maps_general()
        self.load_maps_comunes()
        self.load_maps_diagnoses()

    def load_maps_diagnoses(self):
        diagnoses = pd.read_csv(self.base_path + 'cie-10.csv')
        # Remove descrption and create a dict in format: dict[code] = {'code_0': 'foo', 'code_1: 'bar', etc}
        self.maps['diagnoses'] = diagnoses.drop(columns='description').set_index('code').to_dict('index')

    def load_maps_general(self):
        # TODO: https://pandas.pydata.org/pandas-docs/stable/categorical.html#dtype-in-apply
        # Load categorical codes from csv
        col_codes = pd.read_csv(self.base_path + 'columns_codes.csv', true_values='TRUE', false_values=['FALSE'])
        for type in col_codes.type.unique():
            # build a dictionary with shape: dic[code] = description
            self.maps[type] = col_codes[col_codes.type == type].set_index('code').drop('type', axis=1).value.to_dict()
            # build a pandas CategoricalDtype with ONLY the descriptions
            self.categoricals[type] = CategoricalDtype(col_codes[col_codes.type == type].set_index('code').drop('type', axis=1).value.dropna().values)

    def load_maps_comunes(self):
        comunes = pd.read_excel(
            self.base_path + 'División-Político-Administrativa-y-Servicios-de-Salud-Histórico.xls',
            na_values='Ignorada')
        comunes.drop(columns=comunes.columns[5:], inplace=True)
        comunes.rename(columns={'Nombre Comuna': 'value'}, inplace=True)

        self.maps['comunes_1812_1999'] = comunes\
            .rename(columns={'Código Comuna hasta 1999': 'code'})\
            .set_index('code')\
            .drop(columns=comunes.columns[1:4]).value.to_dict()

        self.maps['comunes_2000_2007'] = comunes\
            .rename(columns={'Código Comuna desde 2000': 'code'})\
            .set_index('code')\
            .drop(columns=comunes.columns[[0,2,3]]).value.to_dict()

        self.maps['comunes_2008_2009'] = comunes\
            .rename(columns={'Código Comuna desde 2008': 'code'})\
            .set_index('code')\
            .drop(columns=comunes.columns[[0, 1, 3]]).value.to_dict()

        self.maps['comunes_2010_2018'] = comunes\
            .rename(columns={'Código Comuna desde 2010': 'code'})\
            .set_index('code')\
            .drop(columns=comunes.columns[[0, 1, 2]]).value.to_dict()


        self.categoricals['comune'] = pd.api.types.CategoricalDtype(comunes.dropna().value.values)

        if self.debug:
            self.log('Creating pickles for: comunes, maps and categoricals')
            pk.dump(comunes, open('comunes.pk', 'wb'))
            pk.dump(self.maps, open("maps.pk", "wb"))
            pk.dump(self.categoricals, open("categoricals.pk", "wb"))
    #
    # row decoder
    #
    def decode___row(self, row, meta=None): #meta for dask compatibilty
        new_row = pd.Series()
        # prepend cols
        for col in self.prepend_cols.keys():
            new_row[col] = self.prepend_cols[col]
        # set the year
        self.dataframe_year = int(row['origin'][:4])
        # run decode_* functions to each column name
        for key,col in row.iteritems():
            key = key.lower()
            if pd.notnull(col):
                try:
                    decodeFunction = getattr(self, 'decode_' + key)
                except AttributeError:
                    if not 'decode_' + key in self.funcnonfund:
                        self.funcnonfund.append('decode_' + key)
                        print('function not found: ' + 'def decode_' + key + '(self, key, column):')
                    # Use self.decode__void to ignore input or
                    # self.decode__pass to pass input "as it" and rememeber to add it @ self.get_meta()
                    decodeFunction = self.decode__pass
                    #decodeFunction = self.decode__void

                try:
                    name, data = decodeFunction(key, col)
                except TypeError as e:
                    self.log('<multi-line error>')
                    self.log('@decodeRow => decode function failed:')
                    self.log('@decodeRow => {}'.format(sys.exc_info()[0]))
                    self.log('@decoderow => {}'.format(e))
                    self.log('@decodeRow => {}, {} = {}({}, {})'.format(name, data, decodeFunction, key, col))
                    self.log('</multi-line error>')
                    #self.log_invalid_type(key, col)
                if name:
                    new_row[name] = data
                    name, data = None, None
            else:
                pass
                #self.log('Ignoring col: {} @ {}, because its empty'.format(key, self.dataframe_year))
        self.cache = {}
        # append cols
        for col in self.append_cols.keys():
            new_row[col] = self.append_cols[col]
        self.append_cols = self.append_cols_base.copy()

        #if type(new_row['deacease_date']) == int:
        #    new_row['deacease_date'] = pd.to_datetime(new_row['deacease_date'])

        self.forcce_datetime_fields(new_row)

        #self.log('deacease_date: {} => {} '.format(new_row['deacease_date'], type(new_row['deacease_date'])))
        # Not necesary without dask
        #
        # delta = self.array_compare(self.get_meta().keys(), new_row.keys())
        # if not delta == '':
        #     print(delta)
        #     sys.exit()
        return new_row
    #
    # abstract decoders
    #
    def decode__void(self, key=None, column=None):
        return False, None

    def decode__pass(self, key, data):
        return key, data

    def decode__diagnosis(self, key, data, prefix='primary'):
        prefix = prefix + '_'

        # Set empty result, and then try to fill it
        self.append_cols[prefix + 'code_0'] = None
        self.append_cols[prefix + 'code_1'] = None
        self.append_cols[prefix + 'code_2'] = None
        self.append_cols[prefix + 'code_3'] = None
        self.append_cols[prefix + 'code_4'] = None
        self.append_cols[prefix + 'code_5'] = None

        # Some strings end with one or more Xs
        def remove_last_x(string):
            if string[-1] == 'X':
                return remove_last_x(string[:-1])
            else:
                return string

        # go trough diagnoses and set them on append_cols
        def diag_lookup_and_set(data):
            # not levels
            ignore_cols = ['source', 'level']
            # set upper level diagnoses
            for d_key in self.maps['diagnoses'][data]:
                if not d_key in ignore_cols:
                    self.append_cols[prefix + d_key] = self.maps['diagnoses'][data][d_key]
            # set the actual diagnosis
            tmp_key = '{}code_{}'.format(prefix, self.maps['diagnoses'][data]['level'])
            self.append_cols[tmp_key] = data

        if type(data) == str:
            data = data.strip()
            data = data.upper()
            data = remove_last_x(data)
            # Found full code
            if data in self.maps['diagnoses']:
                diag_lookup_and_set(data)
            # Once more try with one less character on code
            elif not (data in self.maps['diagnoses']) and ((len(data) == 4) and (data[:-1] in self.maps['diagnoses'])):
                data = data[:-1]
                diag_lookup_and_set(data)
            else:
                self.log('Diagnosis: {} not in the map'.format(data))
                #pk.dump(data, open('not_in_map.pk', 'wb'))

        return False, None


    def decode__int(self, key, column, null_values=None):
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
            #return key, int(column)
            return key, np.float64(column)
        except TypeError as e:
            self.log('Type error: {}'.format(e))
            self.log_invalid_type(key, column)
            return key, None

    def decode__day(self, key, day, name):
        # Define a invalid day response
        def return_none(day, name):
            #self.log('Invalid "{}": {}, assingining 15'.format(name, day))
            self.cache[name + '_day'] = 15
            self.append_cols[name + '_accuracy'] = 'Month'
        # Try to make it in
        try:
            day = int(float(day))
        except:
            return_none(day, name)
        # Fail on known null values
        if day in [99, 0]:
            return_none(day, name)
        else:
            self.cache[name + '_day'] = day
        # Return something
        return False, None

    def decode__month(self, key, month, name):
        # Define a invalid day response
        def return_none(month, name):
            #self.log('Invalid "{}": {}, assingining 15'.format(name, month))
            self.cache[name + '_month'] = 6
            self.append_cols[name + '_accuracy'] = 'Year'
        # Try to make it in
        try:
            month = int(float(month))
        except:
            return_none(month, name)
        # Fail on known null values
        if month in [99, 0]:
            return_none(month, name)
        else:
            self.cache[name + '_month'] = month
        # Return something
        return False, None

    def decode__year(self, key, year, cached_month, cached_day, cached_century=None):
        # self.log('Building date: key={}, year={}, cached_month={}, cached_day={}, cached_century={}'.format(
        #     key,
        #     year,
        #     self.cache[cached_month],
        #     self.cache[cached_day],
        #     cached_century
        # ), True)
        local_nat = pd.NaT
        if type(year) != str:
            if pd.isnull(year):
                self.append_cols[key + '_accuracy'] = None
                return key, local_nat
        if cached_century:
            try:
                year = self.cache[cached_century] * 100 + int(float(year))
            except:
                self.log('Can\'t build a year from {}*100 + int({})'.format(self.cache['born_century'], year))
                return key, local_nat
        else:
            year = int(float(year))


        if year in [0, 9999, 99, 9]:
            self.append_cols[key + '_accuracy'] = None
            self.log('Builded date ({}): {} ({}) => {}-{}-{}'.format(local_nat, key, type(local_nat),
                                                                     year,
                                                                     self.cache[cached_month],
                                                                     self.cache[cached_day])
                     )
            return key, local_nat

        formated_date = '{0}{1:02d}{2:02d}'.format(year, self.cache[cached_month], self.cache[cached_day])
        try:
            #date = datetime.datetime(year, self.cache[cached_month], self.cache[cached_day])
            #date = pd.to_datetime(date, errors='coerce')
            date = pd.to_datetime(formated_date,
                                  format='%Y%m%d',
                                  errors='raise')
        except:
            self.log('Invalid date: datetime.datetime({}) at {}'.format(formated_date,
                                                                 key))
            self.append_cols[key + '_accuracy'] = None
            date = local_nat

        # if int(date) < 0:
        #     self.log('Builded date ({}) from {}'.format(date, formated_date))
        return key, date

    def decode__categorical(self, key, value, name, key_make_string=False, key_make_int=False, map_name=None, cat_from_map=False):
        if type(value) != str:
            if np.isnan(value):
                return name, None
        else:
            # empty string
            if not value.strip():
                return name, None
        if map_name is None:
            map_name = name
        if cat_from_map:
            cat_name = map_name
        else:
            cat_name = name
        if key_make_int:
            value = int(float(value)) #
        if key_make_string:
            value = str(value)
        if value in self.maps[map_name]:
            return name, \
                self.maps[map_name][value]
                #pd.Categorical(self.maps[map_name][value], dtype=self.categoricals[cat_name]) #.codes[0]
        else:
            self.log_invalid_type('{} ({}) '.format(name, key), value, map_name)
            return name, None

    def decode__actividad(self, key, column):
        if type(column) != str:
            if np.isnan(column):
                return key, None
        else:
            column = column.lower()
        try:
            self.cache[key] = int(float(column))
            return self.decode__categorical(key, column, key, True, True, map_name='activity', cat_from_map=True)
        except:
            self.log_invalid_type(key, column)
            return key, None

    def decode__ocupation(self, key, column, cached_activity):
        if type(column) != str:
            if np.isnan(column):
                return key, None
        else:
            # empty string
            if not column.strip():
                return key, None
            column = column.lower()
        if not cached_activity in self.cache:
            return key, None
        if self.cache[cached_activity] == 9:
            return key, None
        try:
            value = '{}.{}'.format(self.cache[cached_activity], column)
        except:
            self.log('Weird shit happening here :(')
            return key, None
        try:
            return self.decode__categorical(key, value, key, map_name='ocupation', cat_from_map=True)
        except TypeError as e:
            self.log(e)
            self.log_invalid_type(key, value)
            return key, None
    #
    # fields decoders
    #

    # bird_date
    def decode_dia_nac(self, key, column):
        return self.decode__day(key, column, 'bird_date')

    def decode_mes_nac(self, key, column):
        return self.decode__month(key, column, 'bird_date')

    def decode_ano1_nac(self, key, column):
        self.cache['bird_date_century'] = int(float(column))
        return False, None

    def decode_ano2_nac(self, key, column):
        return self.decode__year('bird_date', column, 'bird_date_month', 'bird_date_day', 'bird_date_century')

    def decode_ano_nac(self, key, column):
        return self.decode__year('bird_date', column, 'bird_date_month', 'bird_date_day')
    # /bird_date

    def decode_sexo(self, key, column):
        return self.decode__categorical(key, column, 'assigned_sex', True, True)

    def decode_est_civil(self, key, column):
        return self.decode__categorical(key, column, 'marital_status', True, True)

    def decode_edad_tipo(self, key, column):
        return self.decode__categorical(key, column, 'age_type', True, True)

    def decode_peso(self, key, column):
        return self.decode__int('born_weight_grams', column, ['9999', ])

    def decode_gestacion(self, key, column):
        return self.decode__int('gestation_age_weeks', column)

    def decode_edad_cant(self, key, column):
        return self.decode__int('age_amount', column)

    def decode_curso_ins(self, key, column):
        return self.decode__int('formal_education_years', column)

    def decode_curso_m(self, key, column):
        return self.decode__int('mother_formal_education_years', column)

    def decode_curso_ma(self, key, column):
        return self.decode__int('mother_formal_education_years', column)

    def decode_curs_ins_m(self, key, column):
        return self.decode__int('mother_formal_education_years', column)

    def decode_curso_p(self, key, column):
        return self.decode__int('father_formal_education_years', column)

    def decode_curso_pa(self, key, column):
        return self.decode__int('father_formal_education_years', column)

    def decode_curs_ins_p(self, key, column):
        return self.decode__int('father_formal_education_years', column)

    def decode_nivel_ins(self, key, column):
        return self.decode__categorical(key, column, 'formal_education_level', True, True)

    def decode_nive_ins_m(self, key, column):
        return self.decode__categorical(key, column, 'mother_formal_education_level', True, True, 'formal_education_level', True)

    def decode_nivel_m(self, key, column):
        return self.decode__categorical(key, column, 'mother_formal_education_level', True, True, 'formal_education_level', True)

    def decode_nivel_ma(self, key, column):
        return self.decode_nivel_m(key, column)

    def decode_nivel_p(self, key, column):
        return self.decode__categorical(key, column, 'father_formal_education_level', True, True, 'formal_education_level', True)

    def decode_nivel_pa(self, key, column):
        return self.decode__categorical(key, column, 'father_formal_education_level', True, True, 'formal_education_level', True)

    def decode_nive_ins_p(self, key, column):
        return self.decode__categorical(key, column, 'father_formal_education_level', True, True, 'formal_education_level', True)

    def decode_actividad(self, key, column):
        return self.decode__actividad('activity', column)

    def decode_activ(self, key, column):
        return self.decode__actividad('activity', column)

    def decode_categoria(self, key, column):
        return self.decode__categorical(key, column, 'occupational_category', True, True)

    def decode_categ(self, key, column):
        return self.decode__categorical(key, column, 'occupational_category', True, True)

    def decode_categ_m(self, key, column):
        return self.decode__categorical(key, column, 'mother_occupational_category', True, True,
                                        map_name='occupational_category', cat_from_map=True)

    def decode_catego_m(self, key, column):
        return self.decode_categ_m(key, column)

    def decode_categ_ma(self, key, column):
        return self.decode_categ_m(key, column)

    def decode_categ_p(self, key, column):
        return self.decode__categorical(key, column, 'father_occupational_category', True, True,
                                        map_name='occupational_category', cat_from_map=True)

    def decode_catego_p(self, key, column):
        return self.decode_categ_p(key, column)

    def decode_categ_pa(self, key, column):
        return self.decode_categ_p(key, column)

    # deacease date
    def decode_dia_def(self, key, column):
        return self.decode__day(key, column, 'deacease_date')

    def decode_def_dia(self, key, column):
        return self.decode__day(key, column, 'deacease_date')

    def decode_mes_def(self, key, column):
        return self.decode__month(key, column, 'deacease_date')

    def decode_def_mes(self, key, column):
        return self.decode__month(key, column, 'deacease_date')

    def decode_cer_mes(self, key, column):
        return self.decode__month(key, column, 'deacease_date')

    def decode_ano_def(self, key, column):
        return self.decode__year('deacease_date', column, 'deacease_date_month', 'deacease_date_day')

    def decode_cer_ano(self, key, column):
        return self.decode__year('deacease_date', column, 'deacease_date_month', 'deacease_date_day')

    def decode_def_ano(self, key, column):
        return self.decode__year('deacease_date', column, 'deacease_date_month', 'deacease_date_day')

    def decode_lugar_def(self, key, column):
        return self.decode__categorical(key, column, 'decease_place', True, True)

    def decode_glo_ocupa(self, key, column):
        # Field only exists on 2011 dataset with 2444 unique values including:
        # ESTILISTA, PANIFICADOR, JUBILADO, JUBILADA, DUEÑA DE CASA, LABORES DE CASA, SE DESCONOCE, etc.
        return self.decode__void()

    def decode_mv_lugar(self, key, column):
        # Field inly exist on 2011 dataset, and it's empty.
        return self.decode__void()

    def decode_def_local(self, key, column):
        return self.decode__categorical(key, column, 'decease_place', True, True)

    def decode_local_def(self, key, column):
        return self.decode__categorical(key, column, 'decease_place', True, True)

    def decode_loca_def(self, key, column):
        return self.decode__categorical(key, column, 'decease_place', True, True)

    def decode_reg_res(self, key, column):
        return self.decode__categorical(key, column, 'home_region', True, True, 'region', True)

    def decode_serv_res(self, key, column):
        return self.decode__categorical(key, column, 'home_health_service', True, True, 'health_service', True)

    def decode_res_serv(self, key, column):
        return self.decode__categorical(key, column, 'home_health_service', True, True, 'health_service', True)

    def decode_res_reg(self, key, column):
        return self.decode__categorical(key, column, 'home_region', True, True, 'region', True)

    def decode_comuna(self, key, column):
        if self.dataframe_year <= 2000:
            return self.decode__categorical(key, column, 'comune', False, True, map_name='comunes_1812_1999')
        elif self.dataframe_year > 2000 and self.dataframe_year <= 2007:
            return self.decode__categorical(key, column, 'comune', False, True, map_name='comunes_2000_2007')
        elif self.dataframe_year >= 2008 and self.dataframe_year <= 2009:
            return self.decode__categorical(key, column, 'comune', False, True, map_name='comunes_2008_2009')
        elif self.dataframe_year >= 2010 :
            return self.decode__categorical(key, column, 'comune', False, True, map_name='comunes_2010_2018')

    def decode_cod_comuna(self, key, column):
        return self.decode_comuna(key, column)

    def decode_urb_rural(self, key, column):
        return self.decode__categorical(key, column, 'territory_class', True, True)

    def decode_urba_rural(self, key, column):
        return self.decode__categorical(key, column, 'territory_class', True, True)

    def decode_area(self, key, column):
        return self.decode__categorical(key, column, 'territory_class', True, True)

    # Diagnoses
    def decode_diag1(self, key, column):
        return self.decode__diagnosis(key, column)

    def decode_diag2(self, key, column):
        return self.decode__diagnosis(key, column, 'secondary')
    # /Diagnoses

    def decode_at_medica(self, key, column):
        return self.decode__categorical(key, column, 'medical_attention', True, True)

    def decode_cal_medico(self, key, column):
        return self.decode__categorical(key, column, 'reporter_role', True, True)

    def decode_cod_menor(self, key, column):
        return self.decode__categorical(key, column, 'toddler', True, True)

    def decode_nutritivo(self, key, column):
        return self.decode__categorical(key, column, 'nutrition_status', True, True)

    def decode_edad_m(self, key, column):
        return self.decode__int('mother_age', column)

    def decode_edad_p(self, key, column):
        return self.decode__int('father_age', column)

    def decode_edad_padre(self, key, column):
        return self.decode__int('father_age', column)

    def decode_est_civ_m(self, key, column):
        return self.decode__categorical(key, column, 'mother_marital_status', True, True, 'marital_status', True)

    def decode_est_civi_m(self, key, column):
        return self.decode_est_civ_m(key, column)

    def decode_est_civ_ma(self, key, column):
        return self.decode_est_civ_m(key, column)

    def decode_ocupacion(self, key, column):
        return self.decode__ocupation('ocupation', column, 'activity')

    def decode_ocupa(self, key, column):
        return self.decode__ocupation('ocupation', column, 'activity')

    def decode_hij_vivos(self, key, column):
        return self.decode__int('mother_alive_childs', column)

    def decode_hij_fall(self, key, column):
        return self.decode__int('mother_deaceased_childs', column)

    def decode_hij_mort(self, key, column):
        return self.decode__int('mother_stillbirth_childs', column)

    def decode_hij_total(self, key, column):
        return self.decode__int('mother_total_childs', column)

    def decode_parto_abor(self, key, column):
        return self.decode__categorical(key, column, 'bird_abortion', True, True)

    def decode_part_abort(self, key, column):
        return self.decode_parto_abor(key, column)

    # mother_last_bird
    def decode_dia_parto(self, key, column):
        return self.decode__day(key, column, 'mother_last_bird')

    def decode_part_dia(self, key, column):
        return self.decode__day(key, column, 'mother_last_bird')

    def decode_mes_parto(self, key, column):
        return self.decode__month(key, column, 'mother_last_bird')

    def decode_part_mes(self, key, column):
        return self.decode__month(key, column, 'mother_last_bird')

    def decode_ano_parto(self, key, column):
        return self.decode__year('mother_last_bird', column, 'mother_last_bird_month', 'mother_last_bird_day')

    def decode_part_ano(self, key, column):
        return self.decode__year('mother_last_bird', column, 'mother_last_bird_month', 'mother_last_bird_day')
    # / mother_last_bird

    def decode_activ_m(self, key, column):
        return self.decode__actividad('mother_activity', column)

    def decode_activ_ma(self, key, column):
        return self.decode__actividad('mother_activity', column)

    def decode_activ_p(self, key, column):
        return self.decode__actividad('father_activity', column)

    def decode_activ_pa(self, key, column):
        return self.decode__actividad('father_activity', column)

    def decode_ocupa_m(self, key, column):
        return self.decode__ocupation('mother_ocupation', column, 'mother_activity')

    def decode_ocupa_ma(self, key, column):
        return self.decode__ocupation('mother_ocupation', column, 'mother_activity')

    def decode_ocupa_p(self, key, column):
        return self.decode__ocupation('father_ocupation', column, 'father_activity')

    def decode_ocupac_p(self, key, column):
        return self.decode__ocupation('father_ocupation', column, 'father_activity')

    def decode_ocupa_pa(self, key, column):
        return self.decode__ocupation('father_ocupation', column, 'father_activity')

    def decode_origin(self, key, column):
        return self.decode__pass(key, column)

    def decode_fund_causa(self, key, column):
        return self.decode__categorical(key, column, 'diagnosis_source', True, True)


    #
    # utilities
    #
    def log(self, message, do_print=False):
        with open('dataDecode.log', 'a') as f:
            ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
            f.write("{} => ({}) {}\n".format(ts, self.dataframe_year, message))
        if do_print:
            print(message)

    def log_invalid_type(self, key, value, map_name=None):
        self.log('Invalid value for {}: "{}" {} (map: {})'.format(key, value, type(value), map_name))

    def array_compare(self, expecteds, actuals):
        result = ''
        for expected in expecteds:
            if not expected in actuals:
                result += "missing: {}\n".format(expected)
        for actual in actuals:
            if not actual in expecteds:
                result += "unexpected: {}\n".format(actual)
        return result

    def build_categoricals(self, raw_df):
        # This could be optimized, but for now, don't care to copy metada for each category
        meta = self.get_meta()
        for field in meta:
            if meta[field] == 'category':
                raw_df[field] = pd.Series(dtype=meta[field])

    def forcce_datetime_fields(self, series):
        # This could be optimized, but for now, don't care to copy metada for each year
        meta = self.get_meta()

        for field in meta:
            if meta[field] == 'datetime64[ns]':
                if field in series:
                    #print(series[field])
                    #print(type(series[field]))
                    if type(series[field]) == int:
                        series[field] = pd.to_datetime(int(series[field]))

    # Without categoricals
    def get_meta_raw(self):
        return {
            'comune': 'object',
            'diagnosis_source': 'object',
            'origin': 'object',
            'toddler': 'object',
            'bird_date': 'datetime64[ns]',
            'assigned_sex': 'object',
            'marital_status': 'object',
            'age_type': 'object',
            'age_amount': 'int64',
            'formal_education_years': 'int64',
            'formal_education_level': 'object',
            'activity': 'object',
            'ocupation': 'object',
            'occupational_category': 'object',
            'deacease_date': 'datetime64[ns]',
            'decease_place': 'object',
            'home_region': 'object',
            'home_health_service': 'object',
            'territory_class': 'object',
            'medical_attention': 'object',
            'reporter_role': 'object',
            'born_weight_grams': 'int64',
            'gestation_age_weeks': 'int64',
            'nutrition_status': 'object',
            'mother_age': 'int64',
            'mother_marital_status': 'object',
            'mother_alive_childs': 'int64',
            'mother_deaceased_childs': 'int64',
            'mother_stillbirth_childs': 'int64',
            'mother_total_childs': 'int64',
            'bird_abortion': 'object',
            'mother_last_bird': 'datetime64[ns]',
            'mother_activity': 'object',
            'mother_ocupation': 'object',
            'mother_occupational_category': 'object',
            'mother_formal_education_years': 'int64',
            'mother_formal_education_level': 'object',
            'father_age': 'int64',
            'father_activity': 'object',
            'father_ocupation': 'object',
            'father_occupational_category': 'object',
            'father_formal_education_years': 'int64',
            'father_formal_education_level': 'object',
            'bird_date_accuracy': 'object',
            'mother_last_bird_accuracy': 'object',
            'deacease_date_accuracy': 'object',
            'primary_code_0': 'object',
            'primary_code_1': 'object',
            'primary_code_2': 'object',
            'primary_code_3': 'object',
            'primary_code_4': 'object',
            'primary_code_5': 'object',
            'secondary_code_0': 'object',
            'secondary_code_1': 'object',
            'secondary_code_2': 'object',
            'secondary_code_3': 'object',
            'secondary_code_4': 'object',
            'secondary_code_5': 'object',
            }

    def get_meta(self):
        return {'bird_date': 'datetime64[ns]',
                'bird_date_accuracy': str, # TODO: move to categorical
                'assigned_sex': self.categoricals['assigned_sex'],
                'marital_status': self.categoricals['marital_status'],
                'age_type': self.categoricals['age_type'],
                'age_amount': float,
                'formal_education_years': float,
                'formal_education_level': self.categoricals['formal_education_level'],
                'activity': self.categoricals['activity'],
                'ocupation': self.categoricals['ocupation'],
                'occupational_category': self.categoricals['occupational_category'],
                'deacease_date': 'datetime64[ns]',
                'deacease_date_accuracy': str,
                'decease_place': self.categoricals['decease_place'],
                'comune': self.categoricals['comune'],
                #'region': self.categoricals['region'],
                'home_region': self.categoricals['region'],
                #'health_service': self.categoricals['health_service'],
                'home_health_service': self.categoricals['health_service'],
                'territory_class': self.categoricals['territory_class'],
                'diagnosis_source': self.categoricals['diagnosis_source'],
                'medical_attention': self.categoricals['medical_attention'],
                'reporter_role': self.categoricals['reporter_role'],
                'toddler': self.categoricals['toddler'], # Can't be bool because could be None
                'born_weight_grams': float,
                'gestation_age_weeks': float,
                'nutrition_status': self.categoricals['nutrition_status'],
                'mother_age': float,
                'mother_marital_status': self.categoricals['marital_status'],
                'mother_alive_childs': float,
                'mother_deaceased_childs': float,
                'mother_stillbirth_childs': float,
                'mother_total_childs': float,
                'bird_abortion': self.categoricals['bird_abortion'], # Can't be bool because could be None
                'mother_last_bird': 'datetime64[ns]',
                'mother_last_bird_accuracy': str, # TODO: move to categorical
                'mother_activity': self.categoricals['activity'],
                'mother_ocupation': self.categoricals['ocupation'],
                'mother_occupational_category': self.categoricals['occupational_category'],
                'mother_formal_education_years': float,
                'mother_formal_education_level': self.categoricals['formal_education_level'],
                'father_age': float,
                'father_activity': self.categoricals['activity'],
                'father_ocupation': self.categoricals['ocupation'],
                'father_occupational_category': self.categoricals['occupational_category'],
                'father_formal_education_years': float,
                'father_formal_education_level': self.categoricals['formal_education_level'],
                'origin': str, # TODO: move to categorical
                'primary_code_0': str,
                'primary_code_1': str,
                'primary_code_2': str,
                'primary_code_3': str,
                'primary_code_4': str,
                'primary_code_5': str,
                'secondary_code_0': str,
                'secondary_code_1': str,
                'secondary_code_2': str,
                'secondary_code_3': str,
                'secondary_code_4': str,
                'secondary_code_5': str,
                }
