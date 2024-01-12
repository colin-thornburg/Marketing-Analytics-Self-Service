FROM FIRST FILE:

  FOR thisDate IN datesToIterate DO
    
        runDate := thisDate.DAY_DATE;
        dateXWeeksAgo := :runDate - ($lostSalesCalcWeeks*7);

        -- Filter to all zero-sale-blocks that start on the current runDate in loop
        CREATE OR REPLACE TEMPORARY TABLE IDENTIFIER($ROW_ZERO_SALES_BLOCKS_TMP) AS (
            SELECT *
            FROM IDENTIFIER($ZERO_SALES_BLOCKS_TABLE) 
	    . . . 
        );
        
        -- Calculate the total number of OOS days within the last X weeks, for each store-product
        CREATE OR REPLACE TEMPORARY TABLE IDENTIFIER($ROW_OOS_BLOCKS_LAST_XWEEKS_TMP) AS (
            SELECT
                . . .
            FROM IDENTIFIER($OOS_BLOCKS_TABLE)
            . . .
        );
        
        -- Calculate any trailing OOS days from OOS-blocks that span across the date X weeks ago, for each store-product
        CREATE OR REPLACE TEMPORARY TABLE IDENTIFIER($ROW_OOS_BLOCKS_XWEEKS_AGO_TMP) AS (
            SELECT
                . . .
            FROM IDENTIFIER($OOS_BLOCKS_TABLE)
            . . .
        );
        
        -- Calculate the availability-aware expected demand/visits over the last X weeks, and determine whether item was available in current zero-sale-block based on Poisson distribution
        CREATE OR REPLACE TEMPORARY TABLE IDENTIFIER($ROW_OOS_PROBABILITY_TMP) AS (
            SELECT
                . . .
            FROM IDENTIFIER($ROW_ZERO_SALES_BLOCKS_TMP) AS BASE
            LEFT JOIN IDENTIFIER($ROW_OOS_BLOCKS_LAST_XWEEKS_TMP) AS OOS1
                ON BASE.STORE_NBR = OOS1.STORE_NBR
                AND TRY_TO_NUMERIC(BASE.PSKU) = TRY_TO_NUMERIC(OOS1.PSKU)
            LEFT JOIN IDENTIFIER($ROW_OOS_BLOCKS_XWEEKS_AGO_TMP) AS OOS2
                ON BASE.STORE_NBR = OOS2.STORE_NBR
                AND TRY_TO_NUMERIC(BASE.PSKU) = TRY_TO_NUMERIC(OOS2.PSKU)
            LEFT JOIN (
            . . .
        );
        
        -- Insert outputs into OOS history table to feed into the next loop
        INSERT INTO IDENTIFIER($OOS_BLOCKS_TABLE) (
            SELECT
                . . .
            FROM IDENTIFIER($ROW_OOS_PROBABILITY_TMP)
            . . .
        );

    END FOR;
    --RETURN 'Complete';
END

-------------------------------------------
-------------------------------------------

FROM SECOND FILE:

-- 3.1. Initialize empty daily lost sales table, if no prior version exists
CREATE TABLE IF NOT EXISTS IDENTIFIER($LOST_SALES_DAILY_ALL_TABLE) (
    . . .
);

-- 3.2. Explode to daily and insert into historical daily lost sales table
--     (1) For historical runs, this will remove all rows, and then explode/insert all blocks
. . .
DELETE FROM IDENTIFIER($LOST_SALES_DAILY_ALL_TABLE) WHERE TXN_DATE_OF_BLOCK >= $startDate + 1;


INSERT INTO IDENTIFIER($LOST_SALES_DAILY_ALL_TABLE) (
    SELECT DISTINCT
        . . .

    /* Filter to only include OOS blocks that need to be exploded to daily */
    FROM (
        SELECT *
        FROM IDENTIFIER($OOS_BLOCKS_TABLE)
        WHERE OOS_FLAG = 1
            AND TXN_DATE >= $startDate + 1
    ) AS LS

    /* Explode LS table out to daily view, but removing any days where store was closed */
    . . .

    /* Join on PSKU unit price info */
    . . .
);