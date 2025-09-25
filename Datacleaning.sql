SELECT *
FROM layoffs;


CREATE TABLE layoffs_staging
LIKE layoffs;


SELECT *
FROM layoffs_staging;


INSERT layoffs_staging
SELECT *
FROM layoffs;

SELECT *, 
ROW_NUMBER() OVER(
PARTITION BY company,industry, total_laid_off, percentage_laid_off, 'date') as row_num
FROM layoffs_staging;

WITH duplicates_cte AS
(
SELECT * ,
ROW_NUMBER() OVER(
PARTITION BY company, location,
industry, total_laid_off, percentage_laid_off, 'date', stage,
country,funds_raised_millions) as row_num
FROM layoffs_staging
)
SELECT *
FROM duplicates_cte
where row_num > 1;


SELECT *
FROM layoffs_staging
WHERE company = 'oyster';

  

WITH duplicates_cte AS
(
SELECT * ,
ROW_NUMBER() OVER(
PARTITION BY company, location,
industry, total_laid_off, percentage_laid_off, 'date', stage,
country,funds_raised_millions) as row_num
FROM layoffs_staging
)
DELETE 
FROM duplicates_cte
where row_num > 1;

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
SELECT * ,
ROW_NUMBER() OVER(
PARTITION BY company, location,
industry, total_laid_off, percentage_laid_off, 'date', stage,
country,funds_raised_millions) as row_num
FROM layoffs_staging;


DELETE
FROM layoffs_staging2
where row_num > 1;
    
    
SET SQL_SAFE_UPDATES = 0;


-- standardizing data

	SELECT company, TRIM(company)
    FROM layoffs_staging2;


UPDATE Layoffs_staging2
SET company = TRIM(company);
;


SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

SELECT distinct country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2
ORDER BY 1;



update layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

select `date`,
STR_TO_Date(`date`, '%m/%d/%Y')
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = STR_TO_Date(`date`, '%m/%d/%Y');


ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;


SELECT *
FROM layoffs_staging2
where total_laid_off IS NULL
AND percentage_laid_off IS NULL;

select *
FROM layoffs_staging2
where industry = '';

select *
FROM layoffs_staging2
where company Like 'airb%';

update layoffs_staging2
SET industry = NULL
WHERE industry = '';


DELETE 
FROM layoffs_staging2
where total_laid_off IS NULL
AND percentage_laid_off IS NULL;

select *
FROM layoffs_staging2;

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2,company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t12.industry IS NOT NULL;




ALTER TABLE layoffs_staging2
DROP COLUMN row_num;


