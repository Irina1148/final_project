#Луговских Ирина
#Задание 3. Python
#- Реализуйте функцию, которая будет автоматически подгружать информацию из дополнительного файла groups_add.csv (заголовки могут отличаться) и на основании дополнительных параметров пересчитывать метрики.

import pandas as pd
from datetime import timedelta, datetime
import numpy as np
import seaborn as sns
from scipy.stats import chi2_contingency, chi2, levene, ttest_ind, bootstrap

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable

from urllib.parse import urlencode
import requests
import urllib.request
import ssl

default_args = {
    'owner': 'i.lugovskikh',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 10, 20)
}

@dag(default_args=default_args, schedule_interval = '0 12 * * *', catchup=False)
def AB_test_lugovskikh():
    
    path_active_studs = 'https://disk.yandex.ru/d/rI9bG_njef4lvw'
    path_checks = 'https://disk.yandex.ru/d/MCqBhYcc8s3-Hg'
    path_groups = 'https://disk.yandex.ru/d/UiQHWnLgY1YMnw'
    path_group_add = 'https://disk.yandex.ru/d/tQvlY-7I_sOy_A'
    
    
    #функция считывает файл по ссылке с Яндекс Диска
    #возвращает DataFrame
    @task(retries=5)
    def get_data(path, sep):
        url = "https://getfile.dokpub.com/yandex/get/" + path
        context = ssl._create_unverified_context()
        response = urllib.request.urlopen(url, context=context)
        data = pd.read_csv(response, sep=sep)
        return data
    
    #функция для добавления в таблицу с группами данных из дополнительного файла
    #изменяет названия колонок на ['student_id', 'group']
    #объединяет две таблицы, удаляет дубликаты и пропущенные значения
    #находит id_students, которые определены в две разные группы, выводит списком на экран, 
    #и не включает их в результирующую таблицу, возвращает полученную таблицу
    @task()
    def add_data(df, df_add):
        
        def rename_col (df):
            df.columns = ['student_id', 'group']
            return df
        
        group_all = rename_col(df).append(rename_col(df_add)).drop_duplicates().dropna()
        error_list = group_all \
            .groupby('student_id', as_index=False).agg({'group': 'count'}) \
            .query("group > 1").student_id.to_list()
        if len(error_list) > 0:
            print('Список пользователей, которые попали одновременно в две группы:', error_list)
        group_all = group_all.query("student_id != @error_list")
        return group_all
    
    @task()
    def merge_data(active_studs, checks, group_all):
        test = active_studs.merge(checks, how='left', on='student_id') \
                   .merge(group_all, how='inner', on='student_id')
        test.rev = test.rev.astype('float').round()
        test['conv'] = np.where(test.rev > 0, 1, 0)
        return test
    
    @task()
    def CR(test):
        test['conv'] = np.where(test.rev > 0, 1, 0)
        CR_A = (test.query("group == 'A'").conv.sum()/test.query("group == 'A'").conv.count()).round(4)
        CR_B = (test.query("group == 'B'").conv.sum()/test.query("group == 'B'").conv.count()).round(4)
        if CR_A > CR_B:
            res_CR = f'''CR_A > CR_B
CR_A = {CR_A}
CR_B = {CR_B}'''
        else:
            res_CR = f'''CR_A < CR_B
CR_A = {CR_A}
CR_B = {CR_B}'''
        return res_CR
    
    @task()
    def chi2(test):
        pd.crosstab(test.conv, test.group)
        stat, p, dof, expected = chi2_contingency(pd.crosstab(test.conv, test.group))
        if p < 0.05:
            res_chi2 = f'''Гипотезы:
H0 - различия не являются статистически значимыми
H1 - различия являются статистически значимыми
р = {p}
p < 0.05
отклоняем нулевую гипотезу
различия являются статистически значимыми
'''
        else:
            res_chi2 = f'''Гипотезы:
H0 - различия не являются статистически значимыми
H1 - различия являются статистически значимыми
р = {p}
p > 0.05
нет оснований отклонить нулевую гипотезу
различия не являются статистически значимыми
'''
        return res_chi2
            
    @task()
    def ARPPU(test):
        ARPPU_A = (test.query("group == 'A'").rev.sum()/test.query("group == 'A' and conv == 1").rev.count()).round(4)
        ARPPU_B = (test.query("group == 'B'").rev.sum()/test.query("group == 'B' and conv == 1").rev.count()).round(4)
        if ARPPU_A > ARPPU_B:
            res_ARPPU = f'''ARPPU_A > ARPPU_B
ARPPU_A = {ARPPU_A}
ARPPU_B = {ARPPU_B}'''
        else:
            res_ARPPU = f'''ARPPU_A < ARPPU_B
ARPPU_A = {ARPPU_A}
ARPPU_B = {ARPPU_B}'''
        return res_ARPPU
    
    @task()
    def ttest(test):
        test['rev_log'] = np.log(test.rev)
        ARPPU_A = test.query("group == 'A' and rev > 0").rev_log
        ARPPU_B = test.query("group == 'B' and rev > 0").rev_log
        st, p = ttest_ind(ARPPU_A, ARPPU_B)
        if p < 0.05:
            res_ttest = f'''Гипотезы:
H0 - различия не являются статистически значимыми
H1 - различия являются статистически значимыми
р = {p}
p < 0.05
отклоняем нулевую гипотезу
различия являются статистически значимыми
'''
        else:
            res_ttest = f'''Гипотезы:
H0 - различия не являются статистически значимыми
H1 - различия являются статистически значимыми
р = {p}
p > 0.05
нет оснований отклонить нулевую гипотезу
различия не являются статистически значимыми
'''
        return res_ttest
    
    @task()
    def ARPAU(test):
        test.rev = test.rev.fillna(0)
        ARPAU_A = (test.query("group == 'A'").rev.sum()/test.query("group == 'A'").rev.count()).round(4)
        ARPAU_B = (test.query("group == 'B'").rev.sum()/test.query("group == 'B'").rev.count()).round(4)
        if ARPAU_A > ARPAU_B:
            res_ARPAU = f'''ARPAU_A > ARPAU_B
ARPAU_A = {ARPAU_A}
ARPAU_B = {ARPAU_B}'''
        else:
            res_ARPAU = f'''ARPAU_A < ARPAU_B
ARPAU_A = {ARPAU_A}
ARPAU_B = {ARPAU_B}'''
        return res_ARPAU

    @task()
    def bootst(test):
        ARPAU_A = test.query("group == 'A'").rev.fillna(0)
        ARPAU_B = test.query("group == 'B'").rev.fillna(0)
        
        ARPAU_A_ci = list(bootstrap((ARPAU_A, ), np.mean).confidence_interval)
        ARPAU_B_ci = list(bootstrap((ARPAU_B, ), np.mean).confidence_interval)
        
        if ARPAU_A_ci[0] < ARPAU_B_ci[0]:
            if ARPAU_A_ci[1] < ARPAU_B_ci[0]:
                flag = False
            else:
                flag = True
        else:
            if ARPAU_B_ci[1] < ARPAU_A_ci[0]:
                flag = False
            else:
                flag = True
                
        if flag == False:
            res_bootstrap = f'''Гипотезы:
H0 - различия не являются статистически значимыми
H1 - различия являются статистически значимыми
доверительные интервалы:
ARPAU_A = {ARPAU_A_ci}
ARPAU_B = {ARPAU_B_ci}
доверительные интервалы не пересекаются
отклоняем нулевую гипотезу
различия являются статистически значимыми
'''
        else:
            res_bootstrap = f'''Гипотезы:
H0 - различия не являются статистически значимыми
H1 - различия являются статистически значимыми
доверительные интервалы:
ARPAU_A = {ARPAU_A_ci}
ARPAU_B = {ARPAU_B_ci}
доверительные интервалы пересекаются
нет оснований отклонить нулевую гипотезу
различия не являются статистически значимыми
'''
        return res_bootstrap
    
    @task()
    def result(res_CR, res_chi2, res_ARPPU, res_ttest, res_ARPAU, res_bootstrap):
        print(res_CR, res_chi2, res_ARPPU, res_ttest, res_ARPAU, res_bootstrap, sep='\n')
            
        
    active_studs = get_data(path_active_studs, ',')
    checks = get_data(path_checks, ';')
    groups = get_data(path_groups, ';')
    group_add = get_data(path_group_add, ',')
    group_all = add_data(groups, group_add)
    test = merge_data(active_studs, checks, group_all)
    res_CR = CR(test)
    res_chi2 = chi2(test)
    res_ARPPU = ARPPU(test)
    res_ttest = ttest(test)
    res_ARPAU = ARPAU(test)
    res_bootstrap = bootst(test)
    result(res_CR, res_chi2, res_ARPPU, res_ttest, res_ARPAU, res_bootstrap)
    
AB_test_lugovskikh = AB_test_lugovskikh()