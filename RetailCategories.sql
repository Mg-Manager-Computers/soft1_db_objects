/*
        File: RetailCategories.sql
        Purpose: Aggregates retail purchase lines by category and quarter for a given year.
        Usage: Alters procedure [dbo].[RetailCategories] which accepts @YEAR int.
        Output: Quarterly sums and percentages per branch, category, supplier.
        Last modified: 2026-03-09
*/
USE [soft1]
GO
/****** Object:  StoredProcedure [dbo].[RetailCategories]    Script Date: 3/9/2026 1:23:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [dbo].[RetailCategories]
        @YEAR int
AS
BEGIN
SET NOCOUNT ON;

/*
        Temp table: #agores
        - Purpose: captures retail purchase lines filtered by purchase FPRMS values
                and non-export categories to allow aggregation by category, quarter and branch.
        - Columns captured: date, branch/warehouse, supplier, product code, category,
                manufacturer, quantity, price, discount and line value.
*/
drop table if exists #agores
SELECT cast(A.trndate as date)                                  as 'Ημερ/νία',
        FORMAT(sotime,'hh:mm')                                  as 'Ωρα καταχώρησης',
        A.fincode                                               as 'Παραστατικό',
        whouse.NAME                                             as 'Α.Χ.',
        branch.NAME                                             as 'Υποκ/μα',
        E.code                                                  as 'Κωδικός',
        E.NAME                                                  as 'Προμηθευτής',
        Isnull(A.sumamnt, 0)                                    as 'Συνολική',
        D.code                                                  as 'Κωδικός Ε.',
        D.NAME                                                  as 'Επωνυμία',
        mtrc.NAME                                               as 'Εμπορ.κατηγορία',
        mtrm.NAME                                               as 'Κατασκευαστής',
        Isnull(C.qty1, 0)                                       as 'Ποσ.1',
        Isnull(C.price, 0)                                      as 'Τιμή',
        CONVERT(DECIMAL(10,2), Isnull(C.disc1prc, 0)*100)       as 'Εκπτ.%1',
        Isnull(C.lineval, 0)                                    as 'Αξία'
into #agores
FROM   ((((findoc A
           LEFT OUTER JOIN mtrdoc B
                        ON A.findoc = B.findoc)
          LEFT OUTER JOIN mtrlines C
                       ON C.findoc = A.findoc)
         LEFT OUTER JOIN mtrl D
                      ON C.mtrl = D.mtrl)
        LEFT OUTER JOIN trdr E
                     ON A.trdr = E.trdr)
       LEFT OUTER JOIN trdextra F
                    ON A.trdr = F.trdr
        left join (select NAME, mtrcategory from mtrcategory group by NAME, mtrcategory) mtrc on mtrc.MTRCATEGORY = D.mtrcategory
        left join (select NAME, mtrmanfctr from mtrmanfctr group by NAME, mtrmanfctr) mtrm on mtrm.mtrmanfctr = D.mtrmanfctr
        left join (select whouse, NAME from whouse where company = 1000) whouse on whouse.whouse = B.whouse
        left join (select branch, NAME from branch where company = 1000) branch on branch.branch = A.branch
        left join (select NAME, fprms from [dbo].[FPRMS] group by NAME, fprms) fprms on fprms.fprms = A.fprms
WHERE  A.company = 1000
       AND A.sosource = 1251
       AND ( A.fprms IN ( 2061,2062,2066,2069,3261,3262,3263,3264,3267,3268,3269,3270,3279,3280,3281,3282,3283,3284 ) )
       and mtrc.name not like 'EXP%'
       AND A.sodtype = 12
       and year(A.trndate) = @YEAR
       AND Isnull(C.qty1, 0) >= 1
ORDER  BY A.trndate,
          A.findoc

-- Aggregation by year, quarter, branch and category:
-- - Computes total quantity and value per category/branch/quarter
-- - Calculates percentage share of each supplier within the category for the quarter
select  year(ag."Ημερ/νία") 'Έτος', datepart(quarter, ag."Ημερ/νία") 'Τρίμηνο', "Υποκ/μα", ag."Εμπορ.κατηγορία", "Κωδικός", "Προμηθευτής", sum("Ποσ.1") "Συνολική Ποσότητα", sum("Αξία") "Συνολική Αξία", CONVERT(DECIMAL(10,2), (sum("Ποσ.1")/ag2."Συνολική Ποσότητα")*100) "Ποσοστό %", ag2."Συνολική Ποσότητα" 'Σύνολο Αγορασθέντων'
from #agores ag
left join (select "Εμπορ.κατηγορία", sum("Ποσ.1") "Συνολική Ποσότητα", DATEPART(quarter, "Ημερ/νία") "Ημερ/νία" from #agores group by "Εμπορ.κατηγορία", DATEPART(quarter, "Ημερ/νία")) ag2 on ag."Εμπορ.κατηγορία" = ag2."Εμπορ.κατηγορία" and DATEPART(quarter, ag."Ημερ/νία") = ag2."Ημερ/νία"
--left join (select "Εμπορ.κατηγορία", sum("Ποσ.1") "Συνολική Ποσότητα", year("Ημερ/νία") "Ημερ/νία" from #agores group by "Εμπορ.κατηγορία", year("Ημερ/νία")) ag3 on ag."Εμπορ.κατηγορία" = ag3."Εμπορ.κατηγορία" and year(ag."Ημερ/νία") = ag3."Ημερ/νία"
--where ag."Εμπορ.κατηγορία" in ('Laptops', 'Tablets', 'Smartphones', 'TVs', 'Monitors')
group by year(ag."Ημερ/νία"), DATEPART(quarter, ag."Ημερ/νία"), "Υποκ/μα", "Κωδικός", "Προμηθευτής", ag."Εμπορ.κατηγορία", "Συνολική Ποσότητα"
order by ag."Εμπορ.κατηγορία", "Προμηθευτής", datepart(quarter, ag."Ημερ/νία")

END
