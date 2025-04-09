
-- SQL Project - Data Cleaning
-- https://www.kaggle.com/datasets/swaptr/layoffs-2022

SELECT * 
FROM world_layoffs.layoffs;

-- Creating a staging table 

CREATE TABLE layoffs_staging 
LIKE layoffs;

INSERT layoffs_staging 
SELECT * 
FROM layoffs;

-- Data cleaning process:
-- 1. check for duplicates and remove any
-- 2. standardize data and fix errors
-- 3. Analyzing null values
-- 4. remove any columns and rows that are not necessary

-- 1. Remove duplicate rows

SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

SELECT *
FROM
 (
	SELECT *,
	ROW_NUMBER() OVER (
	PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, 
    funds_raised_millions) AS row_num
	FROM layoffs_staging
) 
WHERE row_num > 1;
        
-- As we can't delete from a sub query or CTE so we will make a new table and add a row for row_num in it

SELECT*
FROM layoffs_staging
where company= 'Casper';       
        
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
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
        
SELECT * FROM layoffs_staging2;

INSERT INTO layoffs_staging2
SELECT * ,
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions
			) AS row_num
	FROM 
		layoffs_staging; 
        
        
DELETE FROM layoffs_staging2
WHERE row_num > 1 ;
   
   
-- 2. Standardization

UPDATE layoffs_staging2
SET company = TRIM(company);


SELECT DISTINCT industry
 FROM layoffs_staging2
 ORDER BY 1;
 
 UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';
 
 
 SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
 FROM layoffs_staging2
 ORDER BY 1;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%'; 

 SELECT DISTINCT country
 FROM layoffs_staging2;
 
 SELECT `date`,
 str_to_date(`date` , '%m/%d/%Y' )
 FROM layoffs_staging2;
 
 UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date` , '%m/%d/%Y' )
; 
 
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;


-- 3. Analyzing the null values

-- Poppulating the null values in industry column


SELECT *
FROM layoffs_staging2
WHERE company = 'Airbnb';

UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';

SELECT *
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
  ON t1.company=t2.company
  AND t1.location=t2.location
WHERE t1.industry IS NULL 
AND t2.industry IS NOT NULL; 

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
  ON t1.company=t2.company
  AND t1.location=t2.location
 SET  t1.industry=t2.industry
 WHERE t1.industry IS NULL 
AND t2.industry IS NOT NULL;

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL;



-- 4. Removing unnecessary rows and columns

SELECT *
FROM layoffs_staging2
WHERE  percentage_laid_off IS NULL
AND total_laid_off IS NULL;

DELETE
FROM layoffs_staging2
WHERE  percentage_laid_off IS NULL
AND total_laid_off IS NULL;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

SELECT *
FROM layoffs_staging2;


