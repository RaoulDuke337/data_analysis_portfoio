# Дашборд анализа продаж Adventure Works 

Для создания дашборда был использован стандартный образец данных для Microsoft SQL Server [`Anventure Works 2014 DW`](https://learn.microsoft.com/ru-ru/sql/samples/adventureworks-install-configure?view=sql-server-ver16&tabs=ssms).  
Отчет позволяет проанализировать основные коммерческие показатели компании: **выручку**, **валовую прибыль**, **маржинальность**, **чистую прибыль**, **рентабельность**, в разрезах каналов сбыта, географии продаж, типа магазинов, продукции.

Также, дополнительно, в отчете есть функционал клиентского анализа, система рейтинга магазинов и KPI для сотрудников сбыта.  

Основаная механика проведения анализа показателей в динамике – YoY (год к году), что было обусловлено структурой продаж компании. 

## Сводка
**Отчет:** [`Anventure Works.pbix`](./Adventure%20Works.pbix)   
**Датасет:** [`AdventureWorksDW2014.bak`](./AdventureWorksDW2014.bak)  
**Скилы:** подготовка данных, создание модели данных, концептуализация бизнес-метрик, работа с user story, разработка мер, визуализация данных.  
**Технологии:** T-SQL, Power BI, DAX, SVG.

## Пользовательские сценарии 

Первая страница отчета Overview служит для обзорного анализа продаж компании: мы можем пройтись по основным коммерческим показателям  
сбыта в обобенной детализации и выявить какие-то места, на которые точно стоит обратить внимание менеджменту.

![](./content/Adventure-Works-for-gif-1-footage.gif)

Рассмотрим пару сценариев использования этой страницы:

### 1. Анализ продаж E-com

