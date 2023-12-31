# final_project
### Итоговый проект на курсе Аналитик Данных от Karpov Courses

## Луговских Ирина


**Задание 1. A/B–тестирование**

В ходе тестирования одной гипотезы целевой группе была предложена новая механика оплаты услуг на сайте, у контрольной группы оставалась базовая механика. В качестве задания Вам необходимо проанализировать итоги эксперимента и сделать вывод, стоит ли запускать новую механику оплаты на всех пользователей.

В качестве входных данных Вы имеете 4 csv-файла.

Предлагаем Вам ответить на следующие вопросы:

 - На какие метрики Вы смотрите в ходе анализа и почему?
 - Имеются ли различия в показателях и с чем они могут быть связаны?
 - Являются ли эти различия статистически значимыми?
 - Стоит ли запускать новую механику на всех пользователей?
Данный список вопросов не является обязательным, и Вы можете при своём ответе опираться на собственный план.

**Ход работы:**
Задание выполнено в файле *task_1_3.ipynb*
1. Получение данных и предварительный анализ
2. Создание DataFrame со всеми, нужными для анализа данными
3. Выбор метрик (**CR**, **ARPPU**, **ARPAU**)
4. Исследование изменений значения конверсии (использовала **Критерий Хи-квадрат**)
5. Исследование изменений ARPPU (прологарифмировала колонку *rev* что бы распределение приобрело вид, приближенный к нормальному, использовала **критерия Левена** для проверки гомогенности распределений, а затем использовала **t-test**)
6. Исследование изменений ARPAU (использовала метод **Bootstrap**)


**Стек:**
 - библиотеки: pandas, numpy, seaborn, scipy.stats

**Вывод:**
Новая механика оплаты не влияет на значение конверсии и показатель ARPAU
Новая механика оплаты позволила увеличить значение ARPPU


**Задание 3. Python**

Реализуйте функцию, которая будет автоматически подгружать информацию из дополнительного файла groups_add.csv (заголовки могут отличаться) и на основании дополнительных параметров пересчитывать метрики.
Реализуйте функцию, которая будет строить графики по получаемым метрикам

Задание имеет два решения

1. Первое решение, реализация функций в файле *task_1_3.ipynb*. 
Функция *AB_test()* проводит всю необходимую обработку данных, создает DataFrame *test* со всеми, нужными для анализа данными, исследует изменение метрик (CR, ARPPU, ARPAU), выводит результаты и анализа и выводы на экран и возвращает таблицу *test*.
Функция *grafics(test)* по таблице *test*, полученной функции *AB_test()*, строит графики для метрик CR, ARPPU, ARPAU

2. Второе решение, реализация функций в файле *task_3.py*. 
DAG *AB_test_lugovskikh()* включает в себя следующие task:
 - get_data(path, sep) - получение данных
 - add_data(groups, group_add) - добавление в таблицу с группами данные из дополнительного файла
 - merge_data(active_studs, checks, group_all) - возвращает DataFrame *test* со всеми, нужными для анализа данными 
 - CR(test) - возвращает значения конверсии для двух групп
 - chi2(test) - возвращает вывод на основании использования критерия Хи-квадрат
 - ARPPU(test) - возвращает значения ARPPU для двух групп
 - ttest(test) - возвращает вывод на основании использования t-test
 - ARPAU(test) - возвращает значения ARPAU для двух групп
 - bootst(test) - возвращает вывод на основании использования метода Bootstrap
 - result(res_CR, res_chi2, res_ARPPU, res_ttest, res_ARPAU, res_bootstrap) - выводит на экран все значения и выводы, полученные в функциях CR(test), chi2(test), ARPPU(test), ttest(test), ARPAU(test), bootst(test)

**Стек:**
- airflow
- библиотеки: datetime, airflow, urllib, requests, ssl


**Задание 2. SQL**

Образовательные курсы состоят из различных уроков, каждый из которых состоит из нескольких маленьких заданий. Каждое такое маленькое задание называется "горошиной". Назовём очень усердным учеником того пользователя, который хотя бы раз за текущий месяц правильно решил 20 горошин.

Задача 1.

Дана таблица *default.peas*.

Необходимо написать оптимальный запрос, который даст информацию о количестве очень усердных студентов.NB! Под усердным студентом мы понимаем студента, который правильно решил 20 задач за текущий месяц.

**Ход работы:**
Задание выполнено в файле *task_2.ipynb*
1. Получение данных и предварительный анализ
2. Решение задачи 1

Задача 2.

Образовательная платформа предлагает пройти студентам курсы по модели trial: студент может решить бесплатно лишь 30 горошин в день. Для неограниченного количества заданий в определенной дисциплине студенту необходимо приобрести полный доступ. Команда провела эксперимент, где был протестирован новый экран оплаты.

Даны таблицы: *default.peas*, *default.studs* и *default.final_project_check*.

Необходимо в одном запросе выгрузить следующую информацию о группах пользователей:

 - ARPU 
 - ARPAU 
 - CR в покупку 
 - СR активного пользователя в покупку 
 - CR пользователя из активности по математике (subject = ’math’) в покупку курса по математике
 - ARPU считается относительно всех пользователей, попавших в группы.

Активным считается пользователь, за все время решивший больше 10 задач правильно в любых дисциплинах.
Активным по математике считается пользователь, за все время решивший 2 или больше задач правильно по математике.

Все данные находятся в табличном виде в ClickHouse

**Ход работы:**
Задание выполнено в файле *task_2.ipynb*
1. Получение данных и предварительный анализ
2. Формирование новой таблицы из таблиц *default.peas, default.studs и default.final_project_check* со всеми необходимыми данным для рассчета нужных метрик
3. Рассчет метрик с использованием таблицы из предыдущего шага

**Стек:**
- ClickHouse
- библиотеки: pandas, pandahouse

**Вывод:**

Ответ для задачи 1: 136 очень усердных студентов.
В задаче 2 получены вся необходимая информация одним запросом.
CR для активных студентов по математике показатели отличаются в полтора раза.
Все остальные метрики отличаются примерно в два с половиной раза.
Можно с уверенностью сказать, что внедрение нового экрана оплаты показало хорошие результаты.
