/*
       File: DSV.sql
       Purpose: Stored procedure to produce DSV-specific inventory and suggestion
                                    reports (stock, pending orders, suggestions, Skroutz/MG comparisons).
       Notes: Alters procedure [dbo].[DSV]. No input parameters; uses internal filters
                             for DSV-related warehouses and suppliers.
       Last modified: 2026-03-09
*/
USE [soft1]
GO
/****** Object:  StoredProcedure [dbo].[DSV]    Script Date: 3/9/2026 1:21:25 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [dbo].[DSV]
AS
BEGIN
SET NOCOUNT ON;

/*
       Temp table: #agores
       - Purpose: captures supplier purchase/order lines relevant to DSV flows
              including pricing, uncovered quantities and merchant category metadata.
       - Used to compute expected arrivals (Anamenomena) and supplier-specific aggregates.
       - Filters: company=1000, sosource=1251, sodtype=12
*/
drop table if exists #agores
SELECT pe.NAME                      'Περίοδος',
        Convert(date, A.trndate)    'Date',
        fprms.NAME                  'Τύπος',
        A.fincode                   'Παραστατικό',
        A.seriesnum                 'Αριθμός',
        E.code                      'Κωδικός',
        E.NAME                      'Prom',
        Isnull(A.sumamnt, 0)        'Συνολ.αξία',
        D.code                      'Κωδικός Ε.',
        D.NAME                      'Description',
        D.mtrl                      'mtrl',
        Isnull(C.qty1, 0)           'Amount',
        (CASE WHEN C.RESTMODE IS NOT NULL THEN (C.QTY1-C.QTY1COV-C.QTY1CANC) ELSE 0 END) AS UNCOVQTY,
        Isnull(C.price, 0)          'Price',
        Isnull(D.gweight, 0)        'Μικτό βάρος',
        Isnull(C.lineval, 0)        'Αξία',
        mtrc.NAME                   'Εμπορ.κατηγορία',
        mtrm.NAME                   'Κατασκευαστής',
        E.socurrency          AS X_SOCURRENCY,
        (case when E.socurrency <> 100 then (CASE WHEN Isnull(C.qty, 0) <> 0 THEN CONVERT(DECIMAL(10,2),C.ltrnlineval / C.qty)  ELSE 0 END) else Isnull(C.price, 0) end) as 'Τιμή Ευρώ',
        whouse.NAME           as 'Α.Χ.'
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
        left join (select NAME, PERIOD from [dbo].[PERIOD] group by NAME, PERIOD) pe on pe.PERIOD = A.PERIOD
        left join (select NAME, fprms from [dbo].[FPRMS] group by NAME, fprms) fprms on fprms.fprms = A.fprms
        left join (select whouse, NAME from whouse where company = 1000) whouse on whouse.whouse = B.whouse
WHERE  A.company = 1000
       AND A.sosource = 1251
       --AND ( A.fprms IN ( 3281, 3282, 3283, 3284, 2061,2062,2066,2069,3261,3262,3263,3264,3267,3268,3269,3270,3279,3280,3281,32) )
       --and mtrc.name like 'EXP%'
       and fprms.fprms <> 2020 
       AND A.sodtype = 12

/*
       Temp table: #pwlhseis
       - Purpose: collects recent sales/orders (sosource=1351) for DSV-related fprms
              to calculate recent demand (used in suggestions and minimum quantity logic).
       - Key filters: soredir=0, sodtype=13, qty1 >= 1, recent date window applied later.
*/
drop table if exists #pwlhseis
SELECT 
       cast(A.trndate as date)                          as 'Ημερ/νία',
       FORMAT(sotime,'hh:mm')                           as 'Ωρα καταχώρησης',
       A.fincode                                        as 'Παραστατικό',
       whouse.NAME                                      as 'Α.Χ.',
       branch.NAME                                      as 'Υποκ/μα',
       E.code                                           as 'Κωδικός',
       E.NAME                                           as 'Επωνυμία',
       Isnull(A.sumamnt, 0)                             as 'Συνολική',
       D.code                                           as 'Κωδικός Ε.',
       D.NAME                                           as 'Περιγραφή',
       mtrc.NAME                                        as 'Εμπορ.κατηγορία',
       mtrm.NAME                                        as 'Κατασκευαστής',
       Isnull(C.qty1, 0)                                as 'Ποσ.1',
       a.fprms
into #pwlhseis
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
join (select NAME, mtrmanfctr from mtrmanfctr group by NAME, mtrmanfctr) mtrm on mtrm.mtrmanfctr = D.mtrmanfctr
join (select whouse, NAME from whouse where company = 1000) whouse on whouse.whouse = B.whouse
left join (select finstates, NAME from finstates where ISACTIVE=1 and company = 1000) finstates on finstates.finstates = A.finstates
left join (select branch, NAME from branch where company = 1000) branch on branch.branch = A.branch
WHERE  A.company = 1000
       AND A.sosource = 1351
       AND A.soredir = 0
       AND A.sodtype = 13
       AND Isnull(C.qty1, 0) >= 1
       and a.fprms in (7061,7062,7063,7064,7066,7067,7068,7070,7071,7072,7073,7074,7075,7076,7077,7078,7079,7080,7082,7094,7095,7127,7162,7163,7201,7203,7205,7207,7209,7210,7211,7213,7297,22111)
       and datediff(day, A.trndate, GETDATE()) between 0 and 30

/*
       Temp table: #endodiakinisi
       - Purpose: internal transfers / receipts that affect DSV stock balances.
       - Used to compute 'Διακίνηση MG' and adjust suggestion logic.
*/
drop table if exists #endodiakinisi
SELECT pe.NAME                      'Περίοδος',
        Convert(date, A.trndate)    'Date',
        fprms.NAME                  'Τύπος',
        A.fincode                   'Παραστατικό',
        A.seriesnum                 'Αριθμός',
        E.code                      'Κωδικός',
        E.NAME                      'Prom',
        Isnull(A.sumamnt, 0)        'Συνολ.αξία',
        D.code                      'Κωδικός Ε.',
        D.NAME                      'Description',
        D.mtrl                      'mtrl',
        Isnull(C.qty1, 0)           'Amount',
        (CASE WHEN C.RESTMODE IS NOT NULL THEN (C.QTY1-C.QTY1COV-C.QTY1CANC) ELSE 0 END) AS UNCOVQTY,
        Isnull(C.price, 0)          'Price',
        Isnull(D.gweight, 0)        'Μικτό βάρος',
        Isnull(C.lineval, 0)        'Αξία',
        mtrc.NAME                   'Εμπορ.κατηγορία',
        mtrm.NAME                   'Κατασκευαστής',
        E.socurrency          AS X_SOCURRENCY,
        whouse.NAME           as 'Α.Χ.',
        whouseSEC.NAME        as 'Α.Χ. 2'
into #endodiakinisi
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
        left join (select NAME, PERIOD from [dbo].[PERIOD] group by NAME, PERIOD) pe on pe.PERIOD = A.PERIOD
        left join (select NAME, fprms from [dbo].[FPRMS] group by NAME, fprms) fprms on fprms.fprms = A.fprms
        left join (select whouse, NAME from whouse where company = 1000) whouse on whouse.whouse = B.whouse
        left join (select whouse, NAME from whouse where company = 1000) whouseSEC on whouseSEC.whouse = B.WHOUSESEC
WHERE A.COMPANY=1000 AND A.SOSOURCE=1151 AND A.FPRMS in (2500, 2501) and A.FULLYTRANSF IN (0,2)

/*
       Temp table: #main_table
       - Purpose: consolidated DSV report combining inventory in multiple systems,
              supplier-order lines, Skroutz data and computed suggestion quantities.
       - Contains computed fields: 'Ελάχιστη ποσότητα', 'MG Suggestions', 'Skroutz Suggestions'.
*/
drop table if exists #main_table
-- Consolidated DSV aggregation: joins multiple supplier/retail sources and
-- computes suggestion/minimum quantities. Key fields:
--   'Ελάχιστη ποσότητα' : min qty logic based on category, manufacturer and recent sales
--   'MG Suggestions'     : suggested transfer quantities for MG based on sales vs stock
--   'Skroutz Suggestions': suggested transfers to Skroutz based on recent Skroutz sales
SELECT A.code                                                               as 'Κωδικός',
       isnull(A.CATNAME, '')                                                as 'Κατηγορία',
       isnull(A.MANNAME, '')                                                as 'Κατασκευαστής',
       A.NAME                                                               as 'Περιγραφή',
       isnull(A.cccpartnoexp, '')                                           as 'Part No Export',
       isnull(D.CODE, '')                                                   as 'Κωδικός (συσχέτισης)',
       isnull(cast(A.DSVunitsinstock as varchar), 0.00)                     as 'DSV',
       isnull(cast(A.DSVCustomerOrderUnits as varchar), 0.00)               as 'DSV Δεσμ.',
       --isnull(cast((case when 'ID'+cast(A.ProductID as varchar) = cast(A.SupplierProductID as varchar) then cast((A.DSVSupplierOrderUnits - isnull(A.QTY1, 0)) as varchar) else A.DSVSupplierOrderUnits end)  as varchar), 0.00) as 'Αν.DSV',
       isnull(cast(A.DSVSupplierOrderUnits as int), 0)                      as 'Αν.DSV',
       --isnull(cast(A.SkroutzUnitsinstock as int), 0)                      as 'Υπ.Skroutz',
       --isnull(cast(A.SkroutzCustomerOrderUnits as int), 0)	            as 'Δες.Skroutz',
       --isnull(cast(A.[Ποσ.1] as int), 0)	                                as 'Αναμ.Skroutz',
       isnull(cast(apothema.Apothema as varchar), 0.00)                     as 'MG Stock (συσχέτισης)',
       isnull(cast(anamenomena.Anamenomena as varchar), 0.00)               as 'MG Αναμενόμενα (συσχέτισης)',
       isnull(cast(desmeumena.Desmeumena as varchar), 0.00)                 as 'MG δεσμ (συσχέτισης)',
       --isnull(cast(SDSV.unitsinstock as varchar), 0.00)                   as 'DSV (συσχέτισης)',
       --isnull(cast(DSDSV.SDesmeumena as varchar), 0.00)                   as 'DSV δεσμ (συσχέτισης)',
       --isnull(cast(SADSV.SSupplierOrderUnits as varchar), 0.00)           as ''
       isnull(cast(SSkroutz.SSkroutzUnitsinstock as varchar), 0.00)         as 'Σκρουτζ (συσχέτισης)',
       isnull(cast(SDSkroutz.SSkroutzCustomerOrderUnits as varchar), 0.00)  as 'Δεσ Σκρουτζ (συσχέτισης)',
       isnull(cast(SASkroutz.Anamenomena as varchar), 0.00)                 as 'Αναμ. Σκρουτζ (συσχέτισης)',
       --isnull(mtrbalsheetG3.SALQTY, 0)                                    as 'Gen3',
       --isnull(mtrbalsheetG2.SALQTY, 0)                                    as 'Gen2',
       --isnull(mtrbalsheetG1.SALQTY, 0)                                    as 'Gen1',
       --isnull(mtrbalsheetG.SALQTY, 0)	                                    as 'Gen',
       --isnull(SkroutzG3.SALQTY, 0)                                        as 'SkroutzG3',
       --isnull(SkroutzG2.SALQTY, 0)                                        as 'SkroutzG2',
       --isnull(SkroutzG1.SALQTY, 0)                                        as 'SkroutzG1',
       --isnull(SkroutzG.SALQTY, 0)	                                        as 'SkroutzG',
       --isnull(PapYpG3.SALQTY, 0)                                          as 'PapYpG3',
       --isnull(PapYpG2.SALQTY, 0)                                          as 'PapYpG2',
       --isnull(PapYpG1.SALQTY, 0)                                          as 'PapYpG1',
       --isnull(PapYpG.SALQTY, 0)	                                        as 'PapYpG'
       isnull(SkroutzActualSales.[Ποσ.1], 0)                                as 'Skroutz Actual Sales',
       --MGYPActualSalesNSkroutz.[Ποσ.1]                                      as 'MGYP Actual Sales Non-Skroutz',
       isnull(MGYPActualSales.[Ποσ.1], 0)                                   as 'Total MG Actual Sales',
       isnull(case when (case when (isnull(apothema.Apothema, 0) + isnull(SDSV.unitsinstock, 0) + isnull(SSkroutz.SSkroutzUnitsinstock, 0) + isnull(StockTQL.UnitsInStock, 0) + isnull(StockDreamCargo.UnitsInStock, 0) + isnull(anamenomena.Anamenomena, 0) + isnull(SkroutzActualSales.[Ποσ.1], 0) + isnull(MGYPActualSales.[Ποσ.1], 0)) = 0  and (A.CATNAME like '%aptop%' or A.CATNAME like '%ablet%') and A.MANNAME not in ('Sony', 'Starlink') then 20 
       else (case when (isnull(apothema.Apothema, 0) + isnull(SDSV.unitsinstock, 0) + isnull(SSkroutz.SSkroutzUnitsinstock, 0) + isnull(StockTQL.UnitsInStock, 0) + isnull(StockDreamCargo.UnitsInStock, 0) + isnull(anamenomena.Anamenomena, 0) + isnull(SkroutzActualSales.[Ποσ.1], 0) + isnull(MGYPActualSales.[Ποσ.1], 0)) = 0 and A.MANNAME not in ('Sony', 'Starlink') then 5 else 0 end) 
       end) > isnull(cast(A.DSVunitsinstock as int), 0) + isnull(cast(A.DSVSupplierOrderUnits as int), 0) - isnull(cast(A.DSVCustomerOrderUnits as int), 0)   
       
       then (case when (isnull(cast(A.DSVunitsinstock as int), 0) + isnull(cast(A.DSVSupplierOrderUnits as int), 0) - isnull(cast(A.DSVCustomerOrderUnits as int), 0)) < 0 then 0 else isnull(cast(A.DSVunitsinstock as int), 0) + isnull(cast(A.DSVSupplierOrderUnits as int), 0) - isnull(cast(A.DSVCustomerOrderUnits as int), 0) end)  
       else (case when (isnull(apothema.Apothema, 0) + isnull(SDSV.unitsinstock, 0) + isnull(SSkroutz.SSkroutzUnitsinstock, 0) + isnull(StockTQL.UnitsInStock, 0) + isnull(StockDreamCargo.UnitsInStock, 0) + isnull(anamenomena.Anamenomena, 0) + isnull(SkroutzActualSales.[Ποσ.1], 0) + isnull(MGYPActualSales.[Ποσ.1], 0)) = 0  and (A.CATNAME like '%aptop%' or A.CATNAME like '%ablet%') and A.MANNAME not in ('Sony', 'Starlink') then 20 
       else (case when (isnull(apothema.Apothema, 0) + isnull(SDSV.unitsinstock, 0) + isnull(SSkroutz.SSkroutzUnitsinstock, 0) + isnull(StockTQL.UnitsInStock, 0) + isnull(StockDreamCargo.UnitsInStock, 0) + isnull(anamenomena.Anamenomena, 0) + isnull(SkroutzActualSales.[Ποσ.1], 0) + isnull(MGYPActualSales.[Ποσ.1], 0)) = 0 and A.MANNAME not in ('Sony', 'Starlink') then 5 else 0 end) 
       end) end, 0)                                                                as 'Ελάχιστη ποσότητα',
       cast(round(isnull(case when (case when (case when isnull(MGYPActualSalesNSkroutz.[Ποσ.1], 0) > 0 and A.MANNAME not in ('Sony', 'Starlink')
                then cast(1.5 * isnull(MGYPActualSalesNSkroutz.[Ποσ.1], 0) - (isnull(apothema.Apothema, 0) + isnull(endodiakinisi.Amount, 0) + isnull(anamenomena.Anamenomena, 0) - isnull(desmeumena.Desmeumena, 0)) as decimal(6, 2))
                else 0 end) < 0 then 0 else (case when isnull(MGYPActualSalesNSkroutz.[Ποσ.1], 0) > 0 and A.MANNAME not in ('Sony', 'Starlink')
                then cast(1.5 * isnull(MGYPActualSalesNSkroutz.[Ποσ.1], 0) - (isnull(apothema.Apothema, 0) + isnull(anamenomena.Anamenomena, 0) + isnull(endodiakinisi.Amount, 0) - isnull(desmeumena.Desmeumena, 0)) as decimal(6, 2))
                else 0 end) end) > isnull(cast(A.DSVunitsinstock as int), 0) + isnull(cast(A.DSVSupplierOrderUnits as int), 0) - isnull(cast(A.DSVCustomerOrderUnits as int), 0)  
                then (case when (isnull(cast(A.DSVunitsinstock as int), 0) + isnull(cast(A.DSVSupplierOrderUnits as int), 0) - isnull(cast(A.DSVCustomerOrderUnits as int), 0)) < 0 then 0 else isnull(cast(A.DSVunitsinstock as int), 0) + isnull(cast(A.DSVSupplierOrderUnits as int), 0) - isnull(cast(A.DSVCustomerOrderUnits as int), 0) end)  
                else (case when (case when isnull(MGYPActualSalesNSkroutz.[Ποσ.1], 0) > 0 and A.MANNAME not in ('Sony', 'Starlink')
                then cast(1.5 * isnull(MGYPActualSalesNSkroutz.[Ποσ.1], 0) - (isnull(apothema.Apothema, 0) + isnull(anamenomena.Anamenomena, 0) + isnull(endodiakinisi.Amount, 0) - isnull(desmeumena.Desmeumena, 0)) as decimal(6, 2))
                else 0 end) < 0 then 0 else (case when isnull(MGYPActualSalesNSkroutz.[Ποσ.1], 0) > 0 and A.MANNAME not in ('Sony', 'Starlink')
                then cast(1.5 * isnull(MGYPActualSalesNSkroutz.[Ποσ.1], 0) - (isnull(apothema.Apothema, 0) + isnull(anamenomena.Anamenomena, 0) + isnull(endodiakinisi.Amount, 0) - isnull(desmeumena.Desmeumena, 0)) as decimal(6, 2))
                else 0 end) end) end, 0), 0) as decimal(6, 0))                                                  as 'MG Suggestions',
        isnull(endodiakinisi.Amount, 0)                                                         as 'Διακίνηση MG',
       cast(round(isnull(case when (case when (case when isnull(SkroutzActualSales.[Ποσ.1], 0) > 0 and A.MANNAME not in ('Sony', 'Starlink')
                then cast(isnull(SkroutzActualSales.[Ποσ.1], 0) - ((isnull(SSkroutz.SSkroutzUnitsinstock, 0) + isnull(endodiakinisiSkroutz.Amount, 0) + isnull(SASkroutz.Anamenomena, 0)) - isnull(SDSkroutz.SSkroutzCustomerOrderUnits, 0)) as decimal(6, 2))
                else 0 end) < 0 then 0 else (case when isnull(SkroutzActualSales.[Ποσ.1], 0) > 0 and A.MANNAME not in ('Sony', 'Starlink')
                then cast(isnull(SkroutzActualSales.[Ποσ.1], 0) - ((isnull(SSkroutz.SSkroutzUnitsinstock, 0) + isnull(endodiakinisiSkroutz.Amount, 0) + isnull(SASkroutz.Anamenomena, 0)) - isnull(SDSkroutz.SSkroutzCustomerOrderUnits, 0)) as decimal(6, 2))
                else 0 end) end) > isnull(cast(A.DSVunitsinstock as int), 0) + isnull(cast(A.DSVSupplierOrderUnits as int), 0) - isnull(cast(A.DSVCustomerOrderUnits as int), 0)  
                then (case when (isnull(cast(A.DSVunitsinstock as int), 0) + isnull(cast(A.DSVSupplierOrderUnits as int), 0) - isnull(cast(A.DSVCustomerOrderUnits as int), 0)) < 0 then 0 else (isnull(cast(A.DSVunitsinstock as int), 0) + isnull(cast(A.DSVSupplierOrderUnits as int), 0) - isnull(cast(A.DSVCustomerOrderUnits as int), 0)) end)  
                else (case when (case when isnull(SkroutzActualSales.[Ποσ.1], 0) > 0 and A.MANNAME not in ('Sony', 'Starlink')
                then cast(isnull(SkroutzActualSales.[Ποσ.1], 0) - ((isnull(SSkroutz.SSkroutzUnitsinstock, 0) + isnull(endodiakinisiSkroutz.Amount, 0) + isnull(SASkroutz.Anamenomena, 0)) - isnull(SDSkroutz.SSkroutzCustomerOrderUnits, 0)) as decimal(6, 2))
                else 0 end) < 0 then 0 else (case when isnull(SkroutzActualSales.[Ποσ.1], 0) > 0 and A.MANNAME not in ('Sony', 'Starlink')
                then cast(isnull(SkroutzActualSales.[Ποσ.1], 0) - ((isnull(SSkroutz.SSkroutzUnitsinstock, 0) + isnull(endodiakinisiSkroutz.Amount, 0) + isnull(SASkroutz.Anamenomena, 0)) - isnull(SDSkroutz.SSkroutzCustomerOrderUnits, 0)) as decimal(6, 2))
                else 0 end) end) end, 0), 0) as decimal(6, 0))                                                      as 'Skroutz Suggestions'
         --desmeumena.Desmeumena, SDSkroutz.SSkroutzCustomerOrderUnits,  DSDSV.SDesmeumena, DesmeumenaTQL.CustomerOrderUnits, DesmeumenaDreamCargo.CustomerOrderUnits
         , isnull(endodiakinisiSkroutz.Amount, 0)                                                  as 'Διακίνηση Skroutz'
         , isnull(EikonikaAnamenomena.Anamenomena, 0)                                              as 'Eικονικές Αναμονές'
         --, apothema.Apothema                                                                     as 'Apothema MG'
         --, anamenomena.Anamenomena                                                                 as 'Anamenomena MG'
         --, endodiakinisi.Amount                                                                    as 'Endodiakinisi'
         --, desmeumena.Desmeumena                                                                   as 'Desmeumena MG'
into #main_table
FROM   ((select A.*, mtrcategory.NAME as CATNAME, mtrmanfctr.NAME           as MANNAME, mtrcategory.mtrcategory as MTRCAT
                , DSV.unitsinstock 'DSVunitsinstock', ADSV.[Ποσ.1] 'DSVSupplierOrderUnits', DDSV.Desmeumena 'DSVCustomerOrderUnits'
                , Skroutz.SkroutzUnitsinstock, ASkroutz.[Ποσ.1], DSkroutz.SkroutzCustomerOrderUnits,
                ADSV.[Κωδικός Ε.]--,  ADSV.SupplierProductID
                , ML.QTY1, sohcode
            from mtrl A 
            join (select *
                        from [dbo].[MTRCATEGORY] 
                        WHERE  company = 1000
                        AND isactive = 1
                        AND sodtype = 51) mtrcategory on mtrcategory.MTRCATEGORY = A.MTRCATEGORY
            join (select MTRL from MTRLINES where company = 1000 and WHOUSE = 2050 group by MTRL) MLOR on MLOR.MTRL = A.MTRL 
            left join (select * from [dbo].[MTRMANFCTR] where company = 1000 and isactive = 1) mtrmanfctr on mtrmanfctr.MTRMANFCTR = A.MTRMANFCTR
            left join (select sum(QTY1) QTY1, mtrl
                        from MTRLINES ML 
                        where ML.RESTMODE = 11 and ML.WHOUSE = 2000
                        and ML.company = 1000 and ML.sodtype = 51 and ML.pending = 1
                        group by mtrl
                        ) ML on ML.MTRL = A.MTRL
            left join (select unitsinstock, ProductID from [magicom_shop_2019].[dbo].[Tbl_Products_Suppliers] where supplierid = 43) DSV on cast(DSV.ProductID  as varchar) = A.CODE
            left join (select sum(CustomerOrderUnits) 'Desmeumena', ProductID from [magicom_shop_2019].[dbo].[Tbl_Products_Suppliers] where supplierid = 43 group by ProductID) DDSV on cast(DDSV.ProductID  as varchar) = A.CODE
            --left join (select unitsinstock 'SupplierOrderUnits', ProductID, SupplierProductID from [magicom_shop_2019].[dbo].[Tbl_Products_Suppliers] where supplierid = 18) ADSV on cast(ADSV.SupplierProductID  as varchar) = 'ID' + A.CODE COLLATE SQL_Latin1_General_CP1253_CI_AI
            left join (select p.[Κωδικός Ε.], sum(UNCOVQTY) as [Ποσ.1] from #agores p where "Α.Χ." in ('DSV') and "Τύπος" in ('Παραγγελία Σε Προμηθευτή', 'Packing list προμηθευτή') and UNCOVQTY <> 0 group by p.[Κωδικός Ε.]) ADSV on ADSV.[Κωδικός Ε.] = A.CODE

            left join (select unitsinstock 'SkroutzUnitsinstock', ProductID from [magicom_shop_2019].[dbo].[Tbl_Products_Suppliers] where supplierid = 42) Skroutz on cast(Skroutz.ProductID  as varchar) = A.CODE
            left join (select CustomerOrderUnits 'SkroutzCustomerOrderUnits', ProductID from [magicom_shop_2019].[dbo].[Tbl_Products_Suppliers] where supplierid = 42) DSkroutz on cast(DSkroutz.ProductID  as varchar) = A.CODE
            --left join (select SupplierOrderUnits 'SkroutzSupplierOrderUnits', ProductID from [magicom_shop_2019].[dbo].[Tbl_Products_Suppliers] where supplierid = 42) ASkroutz on cast(ASkroutz.ProductID  as varchar) = A.CODE) A
            left join (select p.[Κωδικός Ε.], sum(UNCOVQTY) as [Ποσ.1] from #agores p where "Α.Χ." in ('Skroutz') and "Τύπος" in ('Παραγγελία Σε Προμηθευτή', 'Packing list προμηθευτή') and UNCOVQTY <> 0 group by p.[Κωδικός Ε.]) ASkroutz on ASkroutz.[Κωδικός Ε.] = A.CODE) A

        LEFT OUTER JOIN mtrdata B ON B.company = 1000 AND A.mtrl = B.mtrl AND B.fiscprd = 2025)
       LEFT OUTER JOIN mtrl D ON A.relitem = D.mtrl
        
       --left join (select sum(SALQTY) SALQTY, MTRL from [dbo].MTRBALSHEET where company = 1000 and year(datefromparts(FISCPRD, PERIOD, 1)) = year(cast(DATEADD(MONTH, -3, GETDATE()) as date)) and month(datefromparts(FISCPRD, PERIOD, 1)) = month(cast(DATEADD(MONTH, -3, GETDATE()) as date)) and PERIOD <> 0 group by MTRL) mtrbalsheetG3 on mtrbalsheetG3.MTRL = D.MTRL
       --left join (select sum(SALQTY) SALQTY, MTRL from [dbo].MTRBALSHEET where company = 1000 and year(datefromparts(FISCPRD, PERIOD, 1)) = year(cast(DATEADD(MONTH, -2, GETDATE()) as date)) and month(datefromparts(FISCPRD, PERIOD, 1)) = month(cast(DATEADD(MONTH, -2, GETDATE()) as date)) and PERIOD <> 0 group by MTRL) mtrbalsheetG2 on mtrbalsheetG2.MTRL = D.MTRL
       --left join (select sum(SALQTY) SALQTY, MTRL from [dbo].MTRBALSHEET where company = 1000 and year(datefromparts(FISCPRD, PERIOD, 1)) = year(cast(DATEADD(MONTH, -1, GETDATE()) as date)) and month(datefromparts(FISCPRD, PERIOD, 1)) = month(cast(DATEADD(MONTH, -1, GETDATE()) as date)) and PERIOD <> 0 group by MTRL) mtrbalsheetG1 on mtrbalsheetG1.MTRL = D.MTRL
       --left join (select sum(SALQTY) SALQTY, MTRL from [dbo].MTRBALSHEET where company = 1000 and year(datefromparts(FISCPRD, PERIOD, 1)) = year(GETDATE()) and month(datefromparts(FISCPRD, PERIOD, 1)) = month(GETDATE()) and PERIOD <> 0 group by MTRL) mtrbalsheetG on mtrbalsheetG.MTRL = D.MTRL
        
       --left join (select sum(SALQTY) SALQTY, MTRL from [dbo].MTRBALSHEET mtrb join (select whouse, NAME from whouse where company = 1000) whouse on whouse.whouse = mtrb.whouse where company = 1000 and year(datefromparts(FISCPRD, PERIOD, 1)) = year(cast(DATEADD(MONTH, -3, GETDATE()) as date)) and month(datefromparts(FISCPRD, PERIOD, 1)) = month(cast(DATEADD(MONTH, -3, GETDATE()) as date)) and PERIOD <> 0 and whouse.NAME = 'Skroutz' group by MTRL) SkroutzG3 on SkroutzG3.MTRL = D.MTRL
       --left join (select sum(SALQTY) SALQTY, MTRL from [dbo].MTRBALSHEET mtrb join (select whouse, NAME from whouse where company = 1000) whouse on whouse.whouse = mtrb.whouse where company = 1000 and year(datefromparts(FISCPRD, PERIOD, 1)) = year(cast(DATEADD(MONTH, -2, GETDATE()) as date)) and month(datefromparts(FISCPRD, PERIOD, 1)) = month(cast(DATEADD(MONTH, -2, GETDATE()) as date)) and PERIOD <> 0 and whouse.NAME = 'Skroutz' group by MTRL) SkroutzG2 on SkroutzG2.MTRL = D.MTRL
       --left join (select sum(SALQTY) SALQTY, MTRL from [dbo].MTRBALSHEET mtrb join (select whouse, NAME from whouse where company = 1000) whouse on whouse.whouse = mtrb.whouse where company = 1000 and year(datefromparts(FISCPRD, PERIOD, 1)) = year(cast(DATEADD(MONTH, -1, GETDATE()) as date)) and month(datefromparts(FISCPRD, PERIOD, 1)) = month(cast(DATEADD(MONTH, -1, GETDATE()) as date)) and PERIOD <> 0 and whouse.NAME = 'Skroutz' group by MTRL) SkroutzG1 on SkroutzG1.MTRL = D.MTRL
       --left join (select sum(SALQTY) SALQTY, MTRL from [dbo].MTRBALSHEET mtrb join (select whouse, NAME from whouse where company = 1000) whouse on whouse.whouse = mtrb.whouse where company = 1000 and year(datefromparts(FISCPRD, PERIOD, 1)) = year(GETDATE()) and month(datefromparts(FISCPRD, PERIOD, 1)) = month(GETDATE()) and PERIOD <> 0 and whouse.NAME = 'Skroutz' group by MTRL) SkroutzG on SkroutzG.MTRL = D.MTRL

       --left join (select sum(SALQTY) SALQTY, MTRL from [dbo].MTRBALSHEET mtrb join (select whouse, NAME from whouse where company = 1000) whouse on whouse.whouse = mtrb.whouse where company = 1000 and year(datefromparts(FISCPRD, PERIOD, 1)) = year(cast(DATEADD(MONTH, -3, GETDATE()) as date)) and month(datefromparts(FISCPRD, PERIOD, 1)) = month(cast(DATEADD(MONTH, -3, GETDATE()) as date)) and PERIOD <> 0 and whouse.NAME in ('Κεντρικός Σολωμού', 'Κεντρικός Παπαδά', 'Κεντρικός Πειραιά', 'Κεντρικός Βουλιαγμένη') group by MTRL) PapYpG3 on PapYpG3.MTRL = D.MTRL
       --left join (select sum(SALQTY) SALQTY, MTRL from [dbo].MTRBALSHEET mtrb join (select whouse, NAME from whouse where company = 1000) whouse on whouse.whouse = mtrb.whouse where company = 1000 and year(datefromparts(FISCPRD, PERIOD, 1)) = year(cast(DATEADD(MONTH, -2, GETDATE()) as date)) and month(datefromparts(FISCPRD, PERIOD, 1)) = month(cast(DATEADD(MONTH, -2, GETDATE()) as date)) and PERIOD <> 0 and whouse.NAME in ('Κεντρικός Σολωμού', 'Κεντρικός Παπαδά', 'Κεντρικός Πειραιά', 'Κεντρικός Βουλιαγμένη') group by MTRL) PapYpG2 on PapYpG2.MTRL = D.MTRL
       --left join (select sum(SALQTY) SALQTY, MTRL from [dbo].MTRBALSHEET mtrb join (select whouse, NAME from whouse where company = 1000) whouse on whouse.whouse = mtrb.whouse where company = 1000 and year(datefromparts(FISCPRD, PERIOD, 1)) = year(cast(DATEADD(MONTH, -1, GETDATE()) as date)) and month(datefromparts(FISCPRD, PERIOD, 1)) = month(cast(DATEADD(MONTH, -1, GETDATE()) as date)) and PERIOD <> 0 and whouse.NAME in ('Κεντρικός Σολωμού', 'Κεντρικός Παπαδά', 'Κεντρικός Πειραιά', 'Κεντρικός Βουλιαγμένη') group by MTRL) PapYpG1 on PapYpG1.MTRL = D.MTRL
       --left join (select sum(SALQTY) SALQTY, MTRL from [dbo].MTRBALSHEET mtrb join (select whouse, NAME from whouse where company = 1000) whouse on whouse.whouse = mtrb.whouse where company = 1000 and year(datefromparts(FISCPRD, PERIOD, 1)) = year(GETDATE()) and month(datefromparts(FISCPRD, PERIOD, 1)) = month(GETDATE()) and PERIOD <> 0 and whouse.NAME in ('Κεντρικός Σολωμού', 'Κεντρικός Παπαδά', 'Κεντρικός Πειραιά', 'Κεντρικός Βουλιαγμένη') group by MTRL) PapYpG on PapYpG.MTRL = D.MTRL
       
       left join (select p.[Κωδικός Ε.], sum(p.[Ποσ.1]) as [Ποσ.1] from #pwlhseis p where p.[Α.Χ.] = 'Skroutz' group by p.[Κωδικός Ε.]) SkroutzActualSales on SkroutzActualSales.[Κωδικός Ε.] = D.CODE
       left join (select p.[Κωδικός Ε.], sum(p.[Ποσ.1]) as [Ποσ.1] from #pwlhseis p where fprms not in (7201, 7205, 7203, 7207) group by p.[Κωδικός Ε.]) MGYPActualSales on MGYPActualSales.[Κωδικός Ε.] = D.CODE
       left join (select p.[Κωδικός Ε.], sum(p.[Ποσ.1]) as [Ποσ.1] from #pwlhseis p where p.[Α.Χ.] <> 'Skroutz' and fprms not in (7201, 7205, 7203, 7207) group by p.[Κωδικός Ε.]) MGYPActualSalesNSkroutz on MGYPActualSalesNSkroutz.[Κωδικός Ε.] = D.CODE

       left join (select sum(unitsinstock) 'Apothema', ProductID from [magicom_shop_2019].[dbo].[Tbl_Products_Suppliers] where SupplierID in (13, 19, 20, 39) group by ProductID) apothema on apothema.ProductID = D.CODE
       --left join (select unitsinstock 'Anamenomena', SupplierProductID from [magicom_shop_2019].[dbo].[Tbl_Products_Suppliers] where SupplierID = 18) anamenomena on anamenomena.SupplierProductID = 'ID'+D.CODE COLLATE SQL_Latin1_General_CP1253_CI_AI
       --left join (select sum(CustomerOrderUnits) 'Desmeumena', SupplierProductID from [magicom_shop_2019].[dbo].[Tbl_Products_Suppliers] group by SupplierProductID) desmeumena on desmeumena.SupplierProductID = D.CODE COLLATE SQL_Latin1_General_CP1253_CI_AI
       left join (select p.[Κωδικός Ε.], sum(UNCOVQTY) as 'Anamenomena' from #agores p where "Α.Χ." in ('Κεντρικός Σολωμού', 'Κεντρικός Παπαδά', 'Κεντρικός Πειραιά', 'Κεντρικός Βουλιαγμένη') and "Τύπος" in ('Παραγγελία Σε Προμηθευτή', 'Packing list προμηθευτή') and UNCOVQTY <> 0 group by p.[Κωδικός Ε.]) anamenomena on anamenomena.[Κωδικός Ε.] = D.CODE
       left join (select p.[Κωδικός Ε.], sum(UNCOVQTY) as 'Anamenomena' from #agores p where "Α.Χ." in ('Κεντρικός Σολωμού', 'Κεντρικός Παπαδά', 'Κεντρικός Πειραιά', 'Κεντρικός Βουλιαγμένη') and "Τύπος" in ('Παραγγελία Σε Προμηθευτή', 'Packing list προμηθευτή') and UNCOVQTY <> 0 and Prom = 'DSV AIR & SEA ΜΟΝΟΠΡΟΣΩΠΗ ΑΝΩΝΥΜΗ ΕΤΑΙΡΕΙΑ' group by p.[Κωδικός Ε.]) EikonikaAnamenomena on EikonikaAnamenomena.[Κωδικός Ε.] = D.CODE

       left join (select COALESCE(SUM(CASE WHEN supplierid IN (13, 19, 20, 39) THEN CustomerOrderUnits ELSE 0 END), 0) 'Desmeumena', ProductID from [magicom_shop_2019].[dbo].[Tbl_Products_Suppliers] GROUP BY productID) desmeumena on desmeumena.productID = D.CODE COLLATE SQL_Latin1_General_CP1253_CI_AI

       left join (select unitsinstock, SupplierProductID from [magicom_shop_2019].[dbo].[Tbl_Products_Suppliers] where supplierid = 43) SDSV on SDSV.SupplierProductID = D.CODE COLLATE SQL_Latin1_General_CP1253_CI_AI
       --left join (select sum(CustomerOrderUnits) 'SDesmeumena', SupplierProductID from [magicom_shop_2019].[dbo].[Tbl_Products_Suppliers] where supplierid = 43 group by SupplierProductID) DSDSV on DSDSV.SupplierProductID = D.CODE COLLATE SQL_Latin1_General_CP1253_CI_AI
       left join (select CustomerOrderUnits 'SDesmeumena', ProductID from [magicom_shop_2019].[dbo].[Tbl_Products_Suppliers] where supplierid = 43) DSDSV on DSDSV.ProductID = D.CODE COLLATE SQL_Latin1_General_CP1253_CI_AI
       --left join (select unitsinstock 'SSupplierOrderUnits', SupplierProductID from [magicom_shop_2019].[dbo].[Tbl_Products_Suppliers] where supplierid = 18) SADSV on SADSV.SupplierProductID = D.CODE COLLATE SQL_Latin1_General_CP1253_CI_AI
       --left join (select p.[Κωδικός Ε.], sum(UNCOVQTY) as 'Anamenomena' from #agores p where "Α.Χ." in ('DSV') and UNCOVQTY <> 0 group by p.[Κωδικός Ε.]) SADSV on SADSV.[Κωδικός Ε.] = D.CODE
       --left join (select p.[Κωδικός Ε.], sum(UNCOVQTY) as 'Anamenomena' from #agores p where "Α.Χ." in ('DSV') and UNCOVQTY <> 0 group by p.[Κωδικός Ε.]) ASDSV on ASDSV.[Κωδικός Ε.] = D.CODE

       left join (select unitsinstock 'SSkroutzUnitsinstock', SupplierProductID from [magicom_shop_2019].[dbo].[Tbl_Products_Suppliers] where supplierid = 42) SSkroutz on SSkroutz.SupplierProductID = D.CODE COLLATE SQL_Latin1_General_CP1253_CI_AI
       --left join (select CustomerOrderUnits 'SSkroutzCustomerOrderUnits', SupplierProductID from [magicom_shop_2019].[dbo].[Tbl_Products_Suppliers] where supplierid = 42) SDSkroutz on SDSkroutz.SupplierProductID = 'ID'+D.CODE COLLATE SQL_Latin1_General_CP1253_CI_AI
       left join (select CustomerOrderUnits 'SSkroutzCustomerOrderUnits', ProductID from [magicom_shop_2019].[dbo].[Tbl_Products_Suppliers] where supplierid = 42) SDSkroutz on SDSkroutz.ProductID = D.CODE COLLATE SQL_Latin1_General_CP1253_CI_AI
       --left join (select SupplierOrderUnits 'SSkroutzSupplierOrderUnits', SupplierProductID from [magicom_shop_2019].[dbo].[Tbl_Products_Suppliers] where supplierid = 42) SASkroutz on SASkroutz.SupplierProductID = 'ID'+D.CODE COLLATE SQL_Latin1_General_CP1253_CI_AI
       left join (select p.[Κωδικός Ε.], sum(UNCOVQTY) as 'Anamenomena' from #agores p where "Α.Χ." in ('Skroutz') and "Τύπος" in ('Παραγγελία Σε Προμηθευτή', 'Packing list προμηθευτή') and UNCOVQTY <> 0 group by p.[Κωδικός Ε.]) SASkroutz on SASkroutz.[Κωδικός Ε.] = D.CODE

        left join (select COALESCE(SUM(CASE WHEN supplierid IN (41) THEN unitsinstock ELSE 0 END), 0) 'UnitsInStock', ProductID from [magicom_shop_2019].[dbo].[Tbl_Products_Suppliers] GROUP BY productID) StockTQL on StockTQL.ProductID = D.CODE
        left join (select COALESCE(SUM(CASE WHEN supplierid IN (41) THEN CustomerOrderUnits ELSE 0 END), 0) 'CustomerOrderUnits', ProductID from [magicom_shop_2019].[dbo].[Tbl_Products_Suppliers] GROUP BY productID) DesmeumenaTQL on DesmeumenaTQL.ProductID = D.CODE

        left join (select COALESCE(SUM(CASE WHEN supplierid IN (22) THEN unitsinstock ELSE 0 END), 0) 'UnitsInStock', ProductID from [magicom_shop_2019].[dbo].[Tbl_Products_Suppliers] GROUP BY productID) StockDreamCargo on StockDreamCargo.ProductID = D.CODE
        left join (select COALESCE(SUM(CASE WHEN supplierid IN (22) THEN CustomerOrderUnits ELSE 0 END), 0) 'CustomerOrderUnits', ProductID from [magicom_shop_2019].[dbo].[Tbl_Products_Suppliers] GROUP BY productID) DesmeumenaDreamCargo on DesmeumenaDreamCargo.ProductID = D.CODE
        
        left join (select "Κωδικός Ε.", sum(Amount) Amount from #endodiakinisi where "Α.Χ." = 'DSV' and "Α.Χ. 2" in ('Κεντρικός Σολωμού', 'Κεντρικός Παπαδά', 'Κεντρικός Πειραιά', 'Κεντρικός Βουλιαγμένη') group by "Κωδικός Ε.") endodiakinisi on endodiakinisi.[Κωδικός Ε.] = D.CODE
        left join (select "Κωδικός Ε.", sum(Amount) Amount from #endodiakinisi where "Α.Χ. 2" = 'Skroutz' group by "Κωδικός Ε.") endodiakinisiSkroutz on endodiakinisiSkroutz.[Κωδικός Ε.] = D.CODE

WHERE  A.company = 1000
       AND A.sodtype = 51
       and (isnull(cast(A.DSVunitsinstock as int), 0) + isnull(A.DSVSupplierOrderUnits, 0)) > 0
ORDER  BY A.code

-- Final projection: formats and renames the #main_table columns into
-- the export-friendly output (quantities, retail comparisons and FIFO purchase price).
select  "Κωδικός",
        "Κατηγορία",
        "Κατασκευαστής",
        "Περιγραφή",
        "Part No Export",
        "Κωδικός (συσχέτισης)",
        convert(int, convert(float, "DSV")) 'DSV',
       convert(int, convert(float, "DSV Δεσμ.")) 'DSV Δεσμ.',
        "Αν.DSV",
        convert(int, convert(float, "MG Stock (συσχέτισης)")) 'MG Stock (συσχέτισης)',
        convert(int, convert(float, "MG Αναμενόμενα (συσχέτισης)")) 'MG Αναμενόμενα (συσχέτισης)',
        convert(int, convert(float, "MG δεσμ (συσχέτισης)")) 'MG δεσμ (συσχέτισης)',
        convert(int, convert(float, "Σκρουτζ (συσχέτισης)")) 'Σκρουτζ (συσχέτισης)',
        convert(int, convert(float, "Δεσ Σκρουτζ (συσχέτισης)")) 'Δεσ Σκρουτζ (συσχέτισης)',
        convert(int, convert(float, "Αναμ. Σκρουτζ (συσχέτισης)")) 'Αναμ. Σκρουτζ (συσχέτισης)',
        "Skroutz Actual Sales",
        "Total MG Actual Sales",
        "Ελάχιστη ποσότητα",
        "MG Suggestions",
        "Διακίνηση MG",
        (case when convert(int, "Skroutz Suggestions") > (convert(float, "DSV") + convert(float, "Αν.DSV") - convert(float, "DSV Δεσμ.") - convert(int, "MG Suggestions")) then (convert(float, "DSV") + convert(float, "Αν.DSV") - convert(float, "DSV Δεσμ.") - convert(int, "MG Suggestions")) else convert(int, "Skroutz Suggestions") end) as 'Skroutz Suggestions',
        "Διακίνηση Skroutz",
        "Eικονικές Αναμονές"
from #main_table
order by "Κωδικός"

drop table if exists #agores
drop table if exists #pwlhseis
drop table if exists #endodiakinisi
drop table if exists #main_table

-- SELECT A.code                                                              as 'Κωδικός',
--        isnull(A.CATNAME, '')                                                as 'Κατηγορία',
--        isnull(A.MANNAME, '')                                                as 'Κατασκευαστής',
--        A.NAME                                                               as 'Περιγραφή',
--        isnull(A.cccpartnoexp, '')                                           as 'Part No Export',
--        isnull(D.CODE, '')                                                   as 'Κωδικός',
--        isnull(cast(A.DSVunitsinstock as varchar), 0.00)                     as 'DSV',
--        isnull(cast(A.DSVCustomerOrderUnits as varchar), 0.00)               as 'DSV Δεσμ.',
--        isnull(cast((case when 'ID'+cast(A.ProductID as varchar) = cast(A.SupplierProductID as varchar) then cast((A.DSVSupplierOrderUnits - isnull(A.QTY1, 0)) as varchar) else A.DSVSupplierOrderUnits end)  as varchar), 0.00) as 'Αν.DSV',
--        isnull(cast(A.SkroutzUnitsinstock as int), 0)                        as 'Υπ.Skroutz',
--        isnull(cast(A.SkroutzCustomerOrderUnits as int), 0)	                as 'Δες.Skroutz',
--        isnull(cast(A.SkroutzSupplierOrderUnits as int), 0)	                as 'Αναμ.Skroutz',
--        isnull(cast(apothema.Apothema as varchar), 0.00)                     as 'MG Stock (συσχέτισης)',
--        isnull(cast(anamenomena.Anamenomena as varchar), 0.00)               as 'MG Αναμενόμενα (συσχέτισης)',
--        isnull(cast(desmeumena.Desmeumena as varchar), 0.00)                 as 'MG δεσμ (συσχέτισης)',
--        isnull(cast(SDSV.unitsinstock as varchar), 0.00)                     as 'DSV (συσχέτισης)',
--        isnull(cast(DSDSV.SDesmeumena as varchar), 0.00)                     as 'DSV δεσμ (συσχέτισης)',
--        --isnull(cast(SADSV.SSupplierOrderUnits as varchar), 0.00)           as '',
--        isnull(cast(SSkroutz.SSkroutzUnitsinstock as varchar), 0.00)         as 'Σκρουτζ (συσχέτισης)',
--        isnull(cast(SDSkroutz.SSkroutzCustomerOrderUnits as varchar), 0.00)  as 'Δεσ Σκρουτζ (συσχέτισης)',
--        isnull(cast(SASkroutz.SSkroutzSupplierOrderUnits as varchar), 0.00)  as 'Αναμ. Σκρουτζ (συσχέτισης)',
--        0                                                                    as 'Συνολική Αποθήκη',
--        isnull(mtrbalsheetG3.SALQTY, 0)                                      as 'Gen3',
--        isnull(mtrbalsheetG2.SALQTY, 0)                                      as 'Gen2',
--        isnull(mtrbalsheetG1.SALQTY, 0)                                      as 'Gen1',
--        isnull(mtrbalsheetG.SALQTY, 0)	                                    as 'Gen',
--        isnull(SkroutzG3.SALQTY, 0)                                      as 'SkroutzG3',
--        isnull(SkroutzG2.SALQTY, 0)                                      as 'SkroutzG2',
--        isnull(SkroutzG1.SALQTY, 0)                                      as 'SkroutzG1',
--        isnull(SkroutzG.SALQTY, 0)	                                    as 'SkroutzG',
--        isnull(PapYpG3.SALQTY, 0)                                        as 'PapYpG3',
--        isnull(PapYpG2.SALQTY, 0)                                        as 'PapYpG2',
--        isnull(PapYpG1.SALQTY, 0)                                        as 'PapYpG1',
--        isnull(PapYpG.SALQTY, 0)	                                        as 'PapYpG'
-- FROM   ((select A.*, mtrcategory.NAME as CATNAME, mtrmanfctr.NAME           as MANNAME, mtrcategory.mtrcategory as MTRCAT
--                 , DSV.unitsinstock 'DSVunitsinstock', ADSV.SupplierOrderUnits 'DSVSupplierOrderUnits', DDSV.Desmeumena 'DSVCustomerOrderUnits'
--                 , Skroutz.SkroutzUnitsinstock, ASkroutz.SkroutzSupplierOrderUnits, DSkroutz.SkroutzCustomerOrderUnits,
--                 ADSV.ProductID,  ADSV.SupplierProductID, ML.QTY1, sohcode
--             from mtrl A 
--             join (select *
--                         from [dbo].[MTRCATEGORY] 
--                         WHERE  company = 1000
--                         AND isactive = 1
--                         AND sodtype = 51) mtrcategory on mtrcategory.MTRCATEGORY = A.MTRCATEGORY
--             join (select MTRL from MTRLINES where company = 1000 and WHOUSE = 2050 group by MTRL) MLOR on MLOR.MTRL = A.MTRL 
--             left join (select * from [dbo].[MTRMANFCTR] where company = 1000 and isactive = 1) mtrmanfctr on mtrmanfctr.MTRMANFCTR = A.MTRMANFCTR
--             left join (select sum(QTY1) QTY1, mtrl
--                         from MTRLINES ML 
--                         where ML.RESTMODE = 11 and ML.WHOUSE = 2000
--                         and ML.company = 1000 and ML.sodtype = 51 and ML.pending = 1
--                         group by mtrl
--                         ) ML on ML.MTRL = A.MTRL
--             left join (select unitsinstock, ProductID from [magicom_shop_2019].[dbo].[Tbl_Products_Suppliers] where supplierid = 43) DSV on cast(DSV.ProductID  as varchar) = A.CODE
--             left join (select sum(CustomerOrderUnits) 'Desmeumena', ProductID from [magicom_shop_2019].[dbo].[Tbl_Products_Suppliers] where supplierid = 43 group by ProductID) DDSV on cast(DDSV.ProductID  as varchar) = A.CODE
--             left join (select unitsinstock 'SupplierOrderUnits', ProductID, SupplierProductID from [magicom_shop_2019].[dbo].[Tbl_Products_Suppliers] where supplierid = 18) ADSV on cast(ADSV.SupplierProductID  as varchar) = 'ID' + A.CODE COLLATE SQL_Latin1_General_CP1253_CI_AI

--             left join (select unitsinstock 'SkroutzUnitsinstock', ProductID from [magicom_shop_2019].[dbo].[Tbl_Products_Suppliers] where supplierid = 42) Skroutz on cast(Skroutz.ProductID  as varchar) = A.CODE
--             left join (select CustomerOrderUnits 'SkroutzCustomerOrderUnits', ProductID from [magicom_shop_2019].[dbo].[Tbl_Products_Suppliers] where supplierid = 42) DSkroutz on cast(DSkroutz.ProductID  as varchar) = A.CODE
--             left join (select SupplierOrderUnits 'SkroutzSupplierOrderUnits', ProductID from [magicom_shop_2019].[dbo].[Tbl_Products_Suppliers] where supplierid = 42) ASkroutz on cast(ASkroutz.ProductID  as varchar) = A.CODE) A
--         LEFT OUTER JOIN mtrdata B ON B.company = 1000 AND A.mtrl = B.mtrl AND B.fiscprd = 2025)
--        LEFT OUTER JOIN mtrl D ON A.relitem = D.mtrl
        
--        left join (select sum(SALQTY) SALQTY, MTRL from [dbo].MTRBALSHEET where company = 1000 and year(datefromparts(FISCPRD, PERIOD, 1)) = year(cast(DATEADD(MONTH, -3, GETDATE()) as date)) and month(datefromparts(FISCPRD, PERIOD, 1)) = month(cast(DATEADD(MONTH, -3, GETDATE()) as date)) and PERIOD <> 0 group by MTRL) mtrbalsheetG3 on mtrbalsheetG3.MTRL = D.MTRL
--        left join (select sum(SALQTY) SALQTY, MTRL from [dbo].MTRBALSHEET where company = 1000 and year(datefromparts(FISCPRD, PERIOD, 1)) = year(cast(DATEADD(MONTH, -2, GETDATE()) as date)) and month(datefromparts(FISCPRD, PERIOD, 1)) = month(cast(DATEADD(MONTH, -2, GETDATE()) as date)) and PERIOD <> 0 group by MTRL) mtrbalsheetG2 on mtrbalsheetG2.MTRL = D.MTRL
--        left join (select sum(SALQTY) SALQTY, MTRL from [dbo].MTRBALSHEET where company = 1000 and year(datefromparts(FISCPRD, PERIOD, 1)) = year(cast(DATEADD(MONTH, -1, GETDATE()) as date)) and month(datefromparts(FISCPRD, PERIOD, 1)) = month(cast(DATEADD(MONTH, -1, GETDATE()) as date)) and PERIOD <> 0 group by MTRL) mtrbalsheetG1 on mtrbalsheetG1.MTRL = D.MTRL
--        left join (select sum(SALQTY) SALQTY, MTRL from [dbo].MTRBALSHEET where company = 1000 and year(datefromparts(FISCPRD, PERIOD, 1)) = year(GETDATE()) and month(datefromparts(FISCPRD, PERIOD, 1)) = month(GETDATE()) and PERIOD <> 0 group by MTRL) mtrbalsheetG on mtrbalsheetG.MTRL = D.MTRL
        
--         left join (select sum(SALQTY) SALQTY, MTRL from [dbo].MTRBALSHEET mtrb join (select whouse, NAME from whouse where company = 1000) whouse on whouse.whouse = mtrb.whouse where company = 1000 and year(datefromparts(FISCPRD, PERIOD, 1)) = year(cast(DATEADD(MONTH, -3, GETDATE()) as date)) and month(datefromparts(FISCPRD, PERIOD, 1)) = month(cast(DATEADD(MONTH, -3, GETDATE()) as date)) and PERIOD <> 0 and whouse.NAME = 'Skroutz' group by MTRL) SkroutzG3 on SkroutzG3.MTRL = D.MTRL
--         left join (select sum(SALQTY) SALQTY, MTRL from [dbo].MTRBALSHEET mtrb join (select whouse, NAME from whouse where company = 1000) whouse on whouse.whouse = mtrb.whouse where company = 1000 and year(datefromparts(FISCPRD, PERIOD, 1)) = year(cast(DATEADD(MONTH, -2, GETDATE()) as date)) and month(datefromparts(FISCPRD, PERIOD, 1)) = month(cast(DATEADD(MONTH, -2, GETDATE()) as date)) and PERIOD <> 0 and whouse.NAME = 'Skroutz' group by MTRL) SkroutzG2 on SkroutzG2.MTRL = D.MTRL
--         left join (select sum(SALQTY) SALQTY, MTRL from [dbo].MTRBALSHEET mtrb join (select whouse, NAME from whouse where company = 1000) whouse on whouse.whouse = mtrb.whouse where company = 1000 and year(datefromparts(FISCPRD, PERIOD, 1)) = year(cast(DATEADD(MONTH, -1, GETDATE()) as date)) and month(datefromparts(FISCPRD, PERIOD, 1)) = month(cast(DATEADD(MONTH, -1, GETDATE()) as date)) and PERIOD <> 0 and whouse.NAME = 'Skroutz' group by MTRL) SkroutzG1 on SkroutzG1.MTRL = D.MTRL
--         left join (select sum(SALQTY) SALQTY, MTRL from [dbo].MTRBALSHEET mtrb join (select whouse, NAME from whouse where company = 1000) whouse on whouse.whouse = mtrb.whouse where company = 1000 and year(datefromparts(FISCPRD, PERIOD, 1)) = year(GETDATE()) and month(datefromparts(FISCPRD, PERIOD, 1)) = month(GETDATE()) and PERIOD <> 0 and whouse.NAME = 'Skroutz' group by MTRL) SkroutzG on SkroutzG.MTRL = D.MTRL

--         left join (select sum(SALQTY) SALQTY, MTRL from [dbo].MTRBALSHEET mtrb join (select whouse, NAME from whouse where company = 1000) whouse on whouse.whouse = mtrb.whouse where company = 1000 and year(datefromparts(FISCPRD, PERIOD, 1)) = year(cast(DATEADD(MONTH, -3, GETDATE()) as date)) and month(datefromparts(FISCPRD, PERIOD, 1)) = month(cast(DATEADD(MONTH, -3, GETDATE()) as date)) and PERIOD <> 0 and whouse.NAME in ('Κεντρικός Σολωμού', 'Κεντρικός Παπαδά', 'Κεντρικός Πειραιά', 'Κεντρικός Βουλιαγμένη') group by MTRL) PapYpG3 on PapYpG3.MTRL = D.MTRL
--         left join (select sum(SALQTY) SALQTY, MTRL from [dbo].MTRBALSHEET mtrb join (select whouse, NAME from whouse where company = 1000) whouse on whouse.whouse = mtrb.whouse where company = 1000 and year(datefromparts(FISCPRD, PERIOD, 1)) = year(cast(DATEADD(MONTH, -2, GETDATE()) as date)) and month(datefromparts(FISCPRD, PERIOD, 1)) = month(cast(DATEADD(MONTH, -2, GETDATE()) as date)) and PERIOD <> 0 and whouse.NAME in ('Κεντρικός Σολωμού', 'Κεντρικός Παπαδά', 'Κεντρικός Πειραιά', 'Κεντρικός Βουλιαγμένη') group by MTRL) PapYpG2 on PapYpG2.MTRL = D.MTRL
--         left join (select sum(SALQTY) SALQTY, MTRL from [dbo].MTRBALSHEET mtrb join (select whouse, NAME from whouse where company = 1000) whouse on whouse.whouse = mtrb.whouse where company = 1000 and year(datefromparts(FISCPRD, PERIOD, 1)) = year(cast(DATEADD(MONTH, -1, GETDATE()) as date)) and month(datefromparts(FISCPRD, PERIOD, 1)) = month(cast(DATEADD(MONTH, -1, GETDATE()) as date)) and PERIOD <> 0 and whouse.NAME in ('Κεντρικός Σολωμού', 'Κεντρικός Παπαδά', 'Κεντρικός Πειραιά', 'Κεντρικός Βουλιαγμένη') group by MTRL) PapYpG1 on PapYpG1.MTRL = D.MTRL
--         left join (select sum(SALQTY) SALQTY, MTRL from [dbo].MTRBALSHEET mtrb join (select whouse, NAME from whouse where company = 1000) whouse on whouse.whouse = mtrb.whouse where company = 1000 and year(datefromparts(FISCPRD, PERIOD, 1)) = year(GETDATE()) and month(datefromparts(FISCPRD, PERIOD, 1)) = month(GETDATE()) and PERIOD <> 0 and whouse.NAME in ('Κεντρικός Σολωμού', 'Κεντρικός Παπαδά', 'Κεντρικός Πειραιά', 'Κεντρικός Βουλιαγμένη') group by MTRL) PapYpG on PapYpG.MTRL = D.MTRL

--        left join (select sum(unitsinstock) 'Apothema', SupplierProductID from [magicom_shop_2019].[dbo].[Tbl_Products_Suppliers] where SupplierID not in (41, 43, 18, 42) group by SupplierProductID) apothema on apothema.SupplierProductID = D.CODE COLLATE SQL_Latin1_General_CP1253_CI_AI
--        left join (select unitsinstock 'Anamenomena', SupplierProductID from [magicom_shop_2019].[dbo].[Tbl_Products_Suppliers] where SupplierID = 18) anamenomena on anamenomena.SupplierProductID = 'ID'+D.CODE COLLATE SQL_Latin1_General_CP1253_CI_AI
--        left join (select sum(CustomerOrderUnits) 'Desmeumena', SupplierProductID from [magicom_shop_2019].[dbo].[Tbl_Products_Suppliers] group by SupplierProductID) desmeumena on desmeumena.SupplierProductID = D.CODE COLLATE SQL_Latin1_General_CP1253_CI_AI

--        left join (select unitsinstock, SupplierProductID from [magicom_shop_2019].[dbo].[Tbl_Products_Suppliers] where supplierid = 43) SDSV on SDSV.SupplierProductID = D.CODE COLLATE SQL_Latin1_General_CP1253_CI_AI
--        left join (select sum(CustomerOrderUnits) 'SDesmeumena', SupplierProductID from [magicom_shop_2019].[dbo].[Tbl_Products_Suppliers] where supplierid = 43 group by SupplierProductID) DSDSV on DSDSV.SupplierProductID = D.CODE COLLATE SQL_Latin1_General_CP1253_CI_AI
--        left join (select unitsinstock 'SSupplierOrderUnits', SupplierProductID from [magicom_shop_2019].[dbo].[Tbl_Products_Suppliers] where supplierid = 18) SADSV on SADSV.SupplierProductID = D.CODE COLLATE SQL_Latin1_General_CP1253_CI_AI

--        left join (select unitsinstock 'SSkroutzUnitsinstock', SupplierProductID from [magicom_shop_2019].[dbo].[Tbl_Products_Suppliers] where supplierid = 42) SSkroutz on SSkroutz.SupplierProductID = D.CODE COLLATE SQL_Latin1_General_CP1253_CI_AI
--        left join (select CustomerOrderUnits 'SSkroutzCustomerOrderUnits', SupplierProductID from [magicom_shop_2019].[dbo].[Tbl_Products_Suppliers] where supplierid = 42) SDSkroutz on SDSkroutz.SupplierProductID = 'ID'+D.CODE COLLATE SQL_Latin1_General_CP1253_CI_AI
--        left join (select SupplierOrderUnits 'SSkroutzSupplierOrderUnits', SupplierProductID from [magicom_shop_2019].[dbo].[Tbl_Products_Suppliers] where supplierid = 42) SASkroutz on SASkroutz.SupplierProductID = 'ID'+D.CODE COLLATE SQL_Latin1_General_CP1253_CI_AI
-- WHERE  A.company = 1000
--        AND A.sodtype = 51
--        and (isnull(cast(A.DSVunitsinstock as int), 0) +
--                 isnull(cast((case when 'ID'+cast(A.ProductID as varchar) = cast(A.SupplierProductID as varchar) then cast((A.DSVSupplierOrderUnits - isnull(A.QTY1, 0)) as varchar) else A.DSVSupplierOrderUnits end) as int), 0)) > 0
--        --and (A.MTRCAT in (309,292,126,286,9,283,23,281,31,32,4,307,256,33,282, 294, 288)
--        --or A.CATNAME in ('Monitors', 'Τροφοδοτικά Cases', 'TVs', 'Cases / Kουτιά Υπολογιστών', 'Monitors Accessories', 'Vacuum Cleaners', 'Modem/routers Ασύρματα', 'Exp Vacuum Cleaners', 'Τσάντες & Θήκες'
--                         --, 'Κάρτες Δικτύου', 'Αξεσουάρ Αναβάθμισης & Ψύκτρες', 'Air Fryers', 'Μπλέντερ', 'EXP Blenders', 'Stick Vacuum Cleaners'))
-- ORDER  BY A.code

END
