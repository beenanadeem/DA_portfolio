-- Exploratory data analysis

SELECT * 
FROM layoffs_staging2;

SELECT MAX(total_laid_off)
FROM layoffs_staging2;

SELECT  MAX(percentage_laid_off), MIN(percentage_laid_off)
FROM layoffs_staging2
WHERE percentage_laid_off IS NOT NULL;

SELECT *
FROM layoffs_staging2
WHERE  percentage_laid_off = 1;

SELECT *
FROM layoffs_staging2
WHERE  percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;

SELECT company, total_laid_off
FROM layoffs_staging2
ORDER BY 2 DESC
LIMIT 5;

SELECT company, SUM(total_laid_off) 
FROM layoffs_staging2
GROUP BY company 
ORDER BY 2 DESC
LIMIT 10
;

SELECT location, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY location
ORDER BY 2 DESC
LIMIT 10;

SELECT country, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY country
ORDER BY 2 DESC;

SELECT YEAR(Date), SUM(Total_laid_off)
FROM layoffs_staging2
GROUP BY YEAR(Date)
ORDER BY 1 ASC;

SELECT industry, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY industry
ORDER BY 2 DESC;


SELECT stage, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY stage
ORDER BY 2 DESC;

-- Rolling total by month
WITH CTE AS
(
SELECT SUBSTRING(Date, 1,7) AS `Month` , SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging2
GROUP BY `Month`
ORDER BY `Month` ASC
)
SELECT `Month`, total_laid_off, SUM(total_laid_off) OVER (ORDER BY `Month`) AS rollong_total
FROM CTE;

-- Year wise rankings of companies with most laidoffs

WITH Company_Year AS 
(
  SELECT company, YEAR(date) AS years, SUM(total_laid_off) AS total_laid_off
  FROM layoffs_staging2
  GROUP BY company, YEAR(date)
)
, Company_Year_Rank AS (
  SELECT company, years, total_laid_off, DENSE_RANK() OVER (PARTITION BY years ORDER BY total_laid_off DESC) AS ranking
  FROM Company_Year
)
SELECT company, years, total_laid_off, ranking
FROM Company_Year_Rank
WHERE ranking <= 3
AND years IS NOT NULL
ORDER BY years ASC, total_laid_off DESC;

