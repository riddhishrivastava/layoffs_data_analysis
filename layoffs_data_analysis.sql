-- Remove duplicates
-- standardize the datas
-- null values or blank values
-- remove any columns
SELECT *
FROM layoffs
;
CREATE TABLE layoffs_staging
LIKE layoffs
;

SELECT *
FROM layoffs_staging
;

INSERT layoffs_staging
SELECT *
FROM layoffs
;

SELECT *, 
ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, `date`, stage, country, funds_raised_millions) as row_num
FROM layoffs_staging
;

WITH duplicate_cte AS
(
SELECT *, 
ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, `date`, stage, country, funds_raised_millions) as row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1
;


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

SELECT *
FROM layoffs_staging2
;

INSERT INTO layoffs_staging2
SELECT *, 
ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, `date`, stage, country, funds_raised_millions) as row_num
FROM layoffs_staging
;

SET SQL_SAFE_UPDATES = 0;

DELETE 
FROM layoffs_staging2
WHERE row_num > 1
;
SET SQL_SAFE_UPDATES = 0;

-- standardizing the data
UPDATE layoffs_staging2
SET company = TRIM(Company)
;

UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = ''
;
UPDATE layoffs_staging2	
SET industry = 'Crypto' 
WHERE industry LIKE 'Crypto%'
;

UPDATE layoffs_staging2	
SET country = 'United States' 
WHERE country LIKE 'United States%'
;

UPDATE layoffs_staging2
SET `date` = str_to_date(`date`, '%m/%d/%Y')
;
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE
;

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL
;

DELETE 
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL
;
SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL
;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num
;


-- EXPLORATORY DATA ANALYSIS
SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY total_laid_off DESC
;

SELECT company, AVG(total_laid_off)
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC
;

SELECT company, MIN(`date`) AS start_date, MAX(`date`) AS end_date
FROM layoffs_staging2
GROUP BY company
;

SELECT YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY YEAR(`date`)
ORDER BY 1 DESC
;

SELECT stage, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY stage
ORDER BY 2 DESC
;

WITH rolling_table AS(
SELECT substr(`date`, 1, 7) AS `month`, SUM(total_laid_off) AS total_off
FROM layoffs_staging2
WHERE substr(`date`, 1, 7) IS NOT NULL
GROUP BY `month`
ORDER BY 1 DESC
)
SELECT `month`, total_off,
SUM(total_off) OVER(ORDER BY `month`)  AS rolling_total
FROM rolling_table
;

