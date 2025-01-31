SELECT * FROM LAYOFFS;

CREATE TABLE LAYOFFS_STAGING
LIKE LAYOFFS;

SELECT * FROM LAYOFFS_STAGING;
INSERT INTO LAYOFFS_STAGING 
SELECT * FROM LAYOFFS;

##REMOVING DUPLICATES

SELECT * ,
ROW_NUMBER() OVER(partition by company ,location , industry, total_laid_off,percentage_laid_off,`date`) as Row_num
from layoffs_staging ;

with CTE_Duplicate as 
(SELECT * ,
ROW_NUMBER() OVER(partition by company ,location , industry, total_laid_off,percentage_laid_off,`date`) 
as Row_num
from layoffs_staging )
select * from CTE_DUPLICATE where row_num>=2;

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int 
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT * FROM LAYOFFS_STAGING2;
INSERT INTO LAYOFFS_STAGING2
SELECT * ,
ROW_NUMBER() OVER(partition by company ,location , industry, total_laid_off,percentage_laid_off,`date`) as Row_num
from layoffs_staging ;

DELETE FROM layoffs_staging2 where row_num>1;

SELECT `date`FROM LAYOFFS_STAGING2;

UPDATE LAYOFFS_STAGING2
SET COMPANY = trim(company);

UPDATE LAYOFFS_STAGING2
SET INDUSTRY = 'CRYPTO'
where industry like 'CRYPTO%';

UPDATE LAYOFFS_STAGING2
SET country =  trim(trailing'.'from country);

UPDATE LAYOFFS_STAGING2
SET `date` = str_to_date (`date`, '%m/%d/%Y');

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` date;

SELECT * FROM LAYOFFS_STAGING2 where company like 'airbnb%';

SELECT * FROM LAYOFFS_STAGING2 where industry is NULL or industry = '';

SELECT t1.industry, t2.industry FROM layoffs_staging2 t1 
join layoffs_staging2 t2 on t1.company=t2.company
where (t1.industry is NULL or t1.industry = '') and 
t2.industry is  NOT NULL;

UPDATE LAYOFFS_STAGING2
SET INDUSTRY = null
where industry like '';

UPDATE LAYOFFS_STAGING2 t1
join layoffs_staging2 t2 
on t1.company=t2.company
SET t1.INDUSTRY = t2.INDUSTRY
where t1.industry is NULL and 
t2.industry is  NOT NULL;

SELECT * FROM layoffs_staging2 where total_laid_off is NULL and percentage_laid_off is null ;

DELETE FROM layoffs_staging2 where total_laid_off is NULL and percentage_laid_off is null ;

ALTER TABLE layoffs_staging2
DROP column row_num;

SELECT * FROM layoffs_staging2;

#Explortory_DATA_ANALYSIS

SELECT COMPANY , SUM(Total_laid_off) from layoffs_staging2 group by company order by 2 DESC ;

SELECT Industry , SUM(Total_laid_off) from layoffs_staging2 group by Industry order by 2 DESC ;

SELECT country , SUM(Total_laid_off) from layoffs_staging2 group by country order by 2 DESC ;

SELECT SUBSTRING(`date`,1,7) as MONTH, SUM(Total_laid_off)  from layoffs_staging2 
where  SUBSTRING(`date`,1,7)  is not NULL
group by MONTH order by 1  ;

with Rolling_total as 
(SELECT SUBSTRING(`date`,1,7) as MONTH, SUM(Total_laid_off) as Total_Laidoff
 from layoffs_staging2 
where  SUBSTRING(`date`,1,7)  is not NULL
group by MONTH order by 1 )
select `month` ,total_laidoff,sum(total_laidoff) over (order by `month`) as rolling_total 
from Rolling_total;

SELECT company , YEAR(`date`), SUM(total_laid_off) from layoffs_staging2
group by company,YEAR(`date`) order by 3 desc;

with company_year(company,years,total_laid_off) as (
SELECT company , YEAR(`date`), SUM(total_laid_off) from layoffs_staging2
group by company,YEAR(`date`) ),
company_rank as (
SELECT *, dense_rank() over (partition by years order by total_laid_off DESC) as ranking 
from company_year where years is not null)
SELECT * FROM COMPANY_RANK WHERE ranking <=5;

























