/*
       File: ExportInvoicesSuppliers.sql
       Purpose: Exports supplier invoice lines for a given year; filters high-value
                                    export invoices and enriches with supplier/country/retail data.
       Usage: Execute the procedure [dbo].[ExportInvoicesSuppliers] with parameter
                             @YEAR int (e.g., 2025).
       Last modified: 2026-03-09
*/
USE [soft1]
GO
/****** Object:  StoredProcedure [dbo].[ExportInvoicesSuppliers]    Script Date: 3/9/2026 1:22:36 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [dbo].[ExportInvoicesSuppliers]
        @YEAR int
AS
BEGIN
SET NOCOUNT ON;

/*
       Temp table: #agores
       - Purpose: collect invoice/packing-list lines for export-related supplier invoices.
       - Captures supplier, product, currency and category metadata used to assemble
              the supplier list and to calculate values in the export report.
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
        Isnull(C.price, 0)          'Price',
        Isnull(D.gweight, 0)        'Μικτό βάρος',
        Isnull(C.lineval, 0)        'Αξία',
        mtrc.NAME                   'Εμπορ.κατηγορία',
        mtrm.NAME                   'Κατασκευαστής',
        E.socurrency          AS X_SOCURRENCY
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
WHERE  A.company = 1000
       AND A.sosource = 1251
       --AND ( A.fprms IN ( 3281, 3282, 3283, 3284, 2061,2062,2066,2069,3261,3262,3263,3264,3267,3268,3269,3270,3279,3280,3281,32) )
       --and mtrc.name like 'EXP%'
       AND A.sodtype = 12

/*
       Temp table: #pwlhseis
       - Purpose: holds export invoice lines (A.sosource=1351) filtered for specific
              export fprms and year (@YEAR). Enriched with retail IDs and FIFO purchase prices.
       - Later used to compute Export stock and Retail comparisons in final SELECT.
*/
drop table if exists #pwlhseis
SELECT 
       cast(A.trndate as date)                          as 'Ημερ/νία',
       FORMAT(sotime,'hh:mm')                           as 'Ωρα καταχώρησης',
       A.fincode                                        as 'Παραστατικό',
       whouse.NAME                                      as 'Α.Χ.',
       branch.NAME                                      as 'Υποκ/μα',
       E.code                                           as 'Κωδικός',
       country.NAME                                     as 'Χώρα',
       E.NAME                                           as 'Επωνυμία',
       Isnull(A.sumamnt, 0)                             as 'Συνολική',
       D.code                                           as 'Κωδικός Ε.',
       D.NAME                                           as 'Περιγραφή',
       D.CCCPARTNOEXP                                   as 'PART NUMBER',
       mtrc.NAME                                        as 'Εμπορ.κατηγορία',
       mtrm.NAME                                        as 'Κατασκευαστής',
       Isnull(C.qty1, 0)                                as 'Ποσ.1',
       (case when E.socurrency <> 100 then (CASE WHEN Isnull(C.qty, 0) <> 0 THEN CONVERT(DECIMAL(10,2),C.ltrnlineval / C.qty)  ELSE 0 END) else Isnull(C.price, 0) end) as 'Τιμή Ευρώ',
       (case when E.socurrency <> 100 then Isnull(C.price, 0) else 0 end) as 'Τιμή Δολάριο',
       --replace((case when E.socurrency = 113 then Isnull(C.price, 0) else '' end), 0, '')                               as 'Τιμή Δολάριο',
       Isnull(C.disc1prc, 0)                            as 'Εκπτ.%1',
       Isnull(C.lineval, 0) as 'Αξία',
       --replace((case when E.socurrency = 100 then Isnull(C.lineval, 0) else '' end), 0, '')                           as 'Αξία Ευρώ',
       --replace((case when E.socurrency = 113 then Isnull(C.lineval, 0) else '' end), 0, '')                             as 'Αξία Δολάριο',
       finstates.NAME                                   as 'Κατάσταση',
       CONVERT(DECIMAL(10,0), mtrp.PURFIFO)             as 'FIFO Αγοράς',
       --[TextToColumns](Promitheutes, ',')             as 'Προμηθευτές'
       isnull(sysx.CODE, '')                            as 'Retail MG ID',
       (case when sysx.CODE is null then '' else cast(sysx.PriceToSell as varchar) end)                 as 'Net Retail Price',
       (case when sysx.CODE is null then '' else cast(isnull(mtrbalsheetG3.SALQTY, 0) as varchar) end) as 'Month -3',
       (case when sysx.CODE is null then '' else cast(isnull(mtrbalsheetG2.SALQTY, 0) as varchar) end) as 'Month -2',
       (case when sysx.CODE is null then '' else cast(isnull(mtrbalsheetG1.SALQTY, 0) as varchar) end) as 'Month -1',
       (case when sysx.CODE is null then '' else cast(isnull(mtrbalsheetG.SALQTY, 0) as varchar) end) as 'This Month',
       Promitheutes                                     as 'Προμηθευτές'
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
left join (select NAME, mtrmanfctr from mtrmanfctr group by NAME, mtrmanfctr) mtrm on mtrm.mtrmanfctr = D.mtrmanfctr
left join (select whouse, NAME from whouse where company = 1000) whouse on whouse.whouse = B.whouse
left join (select COUNTRY, NAME from [dbo].[COUNTRY]) country on country.COUNTRY = E.COUNTRY
left join (select finstates, NAME from finstates where ISACTIVE=1 and company = 1000) finstates on finstates.finstates = A.finstates
left join (select branch, NAME from branch where company = 1000) branch on branch.branch = A.branch
left join (select ag.mtrl
                    , STUFF((select Distinct ', ' + Prom from #agores ag2 where ag2.Description = ag.Description FOR XML PATH ('')),1,2,'') AS 'Promitheutes'
                    , "Κωδικός Ε."
                    , "Εμπορ.κατηγορία"
            from #agores ag
            --where year(Date) in (2023, 2024, 2025)
            group by Description, ag.mtrl, "Κωδικός Ε.", "Εμπορ.κατηγορία") ag on ag.mtrl = D.mtrl
left join(select mtrl,PURFIFO, FISCPRD from MTRCPRICES where PERIOD = 1000 and company = 1000) mtrp on mtrp.mtrl = C.mtrl and mtrp.FISCPRD = year(A.trndate)
left join(select m.CODE, m.mtrl, p.PriceToSell 
            from mtrl m
            --left join [magicom_shop_2019].[dbo].[Tbl_Products] p on p.Descr = m.NAME COLLATE SQL_Latin1_General_CP1253_CI_AI
            --left join [magicom_shop_2019].[dbo].[Tbl_Products] p on p.Descr like '%' + m.NAME COLLATE SQL_Latin1_General_CP1253_CI_AI + '%'
            left join [magicom_shop_2019].[dbo].[Tbl_Products] p on p.ProductID = m.CODE) sysx on  sysx.mtrl = D.RELITEM
left join (select sum(SALQTY) SALQTY, MTRL from [dbo].MTRBALSHEET where company = 1000 and year(datefromparts(FISCPRD, PERIOD, 1)) = year(cast(DATEADD(MONTH, -3, GETDATE()) as date)) and month(datefromparts(FISCPRD, PERIOD, 1)) = month(cast(DATEADD(MONTH, -3, GETDATE()) as date)) and PERIOD <> 0 group by MTRL) mtrbalsheetG3 on mtrbalsheetG3.MTRL = sysx.MTRL
left join (select sum(SALQTY) SALQTY, MTRL from [dbo].MTRBALSHEET where company = 1000 and year(datefromparts(FISCPRD, PERIOD, 1)) = year(cast(DATEADD(MONTH, -2, GETDATE()) as date)) and month(datefromparts(FISCPRD, PERIOD, 1)) = month(cast(DATEADD(MONTH, -2, GETDATE()) as date)) and PERIOD <> 0 group by MTRL) mtrbalsheetG2 on mtrbalsheetG2.MTRL = sysx.MTRL
left join (select sum(SALQTY) SALQTY, MTRL from [dbo].MTRBALSHEET where company = 1000 and year(datefromparts(FISCPRD, PERIOD, 1)) = year(cast(DATEADD(MONTH, -1, GETDATE()) as date)) and month(datefromparts(FISCPRD, PERIOD, 1)) = month(cast(DATEADD(MONTH, -1, GETDATE()) as date)) and PERIOD <> 0 group by MTRL) mtrbalsheetG1 on mtrbalsheetG1.MTRL = sysx.MTRL
left join (select sum(SALQTY) SALQTY, MTRL from [dbo].MTRBALSHEET where company = 1000 and year(datefromparts(FISCPRD, PERIOD, 1)) = year(GETDATE()) and month(datefromparts(FISCPRD, PERIOD, 1)) = month(GETDATE()) and PERIOD <> 0 group by MTRL) mtrbalsheetG on mtrbalsheetG.MTRL = sysx.MTRL
WHERE  A.company = 1000
       AND A.sosource = 1351
       AND A.soredir = 0
       AND A.fprms IN (7201, 7205, 7203, 7207)
       --and mtrc.name like 'EXP%'
       AND A.sodtype = 13
       AND Isnull(C.qty1, 0) >= 1
       and year(A.trndate) = @YEAR
       and Isnull(C.lineval, 0) > 2500
-- UNION branch: includes additional export invoice lines that meet alternate
-- conditions (different sosource/fprms logic). Columns remain the same so
-- results can be concatenated with the first part for the final export.
union
SELECT 
       cast(A.trndate as date)                          as 'Ημερ/νία',
       FORMAT(sotime,'hh:mm')                           as 'Ωρα καταχώρησης',
       A.fincode                                        as 'Παραστατικό',
       whouse.NAME                                      as 'Α.Χ.',
       branch.NAME                                      as 'Υποκ/μα',
       E.code                                           as 'Κωδικός',
       country.NAME                                     as 'Χώρα',
       E.NAME                                           as 'Επωνυμία',
       Isnull(A.sumamnt, 0)                             as 'Συνολική',
       D.code                                           as 'Κωδικός Ε.',
       D.NAME                                           as 'Περιγραφή',
       D.CCCPARTNOEXP                                   as 'PART NUMBER',
       mtrc.NAME                                        as 'Εμπορ.κατηγορία',
       mtrm.NAME                                        as 'Κατασκευαστής',
       Isnull(C.qty1, 0)                                as 'Ποσ.1',
       (case when E.socurrency <> 100 then (CASE WHEN Isnull(C.qty, 0) <> 0 THEN CONVERT(DECIMAL(10,2),C.ltrnlineval / C.qty)  ELSE 0 END) else Isnull(C.price, 0) end) as 'Τιμή Ευρώ',
       (case when E.socurrency <> 100 then Isnull(C.price, 0) else 0 end) as 'Τιμή Δολάριο',
       --replace((case when E.socurrency = 113 then Isnull(C.price, 0) else '' end), 0, '')                               as 'Τιμή Δολάριο',
       CONVERT(DECIMAL(10,2), Isnull(C.disc1prc, 0))                           as 'Εκπτ.%1',
       Isnull(C.lineval, 0) as 'Αξία',
       --replace((case when E.socurrency = 100 then Isnull(C.lineval, 0) else '' end), 0, '')                           as 'Αξία Ευρώ',
       --replace((case when E.socurrency = 113 then Isnull(C.lineval, 0) else '' end), 0, '')                             as 'Αξία Δολάριο',
       finstates.NAME                                   as 'Κατάσταση',
       CONVERT(DECIMAL(10,0), mtrp.PURFIFO)             as 'FIFO Αγοράς',
       --[TextToColumns](Promitheutes, ',')             as 'Προμηθευτές'
       isnull(sysx.CODE, '')                            as 'Retail MG ID',
       (case when sysx.CODE is null then '' else cast(sysx.PriceToSell as varchar) end)                 as 'Net Retail Price',
       (case when sysx.CODE is null then '' else cast(isnull(mtrbalsheetG3.SALQTY, 0) as varchar) end) as 'Month -3',
       (case when sysx.CODE is null then '' else cast(isnull(mtrbalsheetG2.SALQTY, 0) as varchar) end) as 'Month -2',
       (case when sysx.CODE is null then '' else cast(isnull(mtrbalsheetG1.SALQTY, 0) as varchar) end) as 'Month -1',
       (case when sysx.CODE is null then '' else cast(isnull(mtrbalsheetG.SALQTY, 0) as varchar) end) as 'This Month',
       Promitheutes                                     as 'Προμηθευτές'
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
left join (select COUNTRY, NAME from [dbo].[COUNTRY]) country on country.COUNTRY = E.COUNTRY
left join (select finstates, NAME from finstates where ISACTIVE=1 and company = 1000) finstates on finstates.finstates = A.finstates
left join (select branch, NAME from branch where company = 1000) branch on branch.branch = A.branch
left join (select ag.mtrl
                    , STUFF((select Distinct ', ' + Prom from #agores ag2 where ag2.Description = ag.Description FOR XML PATH ('')),1,2,'') AS 'Promitheutes'
                    , "Κωδικός Ε."
                    , "Εμπορ.κατηγορία"
            from #agores ag
            --where year(Date) in (2023, 2024, 2025)
            group by Description, ag.mtrl, "Κωδικός Ε.", "Εμπορ.κατηγορία") ag on ag.mtrl = D.mtrl
left join(select mtrl,PURFIFO, FISCPRD from MTRCPRICES where PERIOD = 1000 and company = 1000) mtrp on mtrp.mtrl = C.mtrl and mtrp.FISCPRD = year(A.trndate)
left join(select m.CODE, m.mtrl, p.PriceToSell 
            from mtrl m
            --left join [magicom_shop_2019].[dbo].[Tbl_Products] p on p.Descr = m.NAME COLLATE SQL_Latin1_General_CP1253_CI_AI
            --left join [magicom_shop_2019].[dbo].[Tbl_Products] p on p.Descr like '%' + m.NAME COLLATE SQL_Latin1_General_CP1253_CI_AI + '%'
            left join [magicom_shop_2019].[dbo].[Tbl_Products] p on p.ProductID = m.CODE) sysx on  sysx.mtrl = D.RELITEM
left join (select sum(SALQTY) SALQTY, MTRL from [dbo].MTRBALSHEET where company = 1000 and year(datefromparts(FISCPRD, PERIOD, 1)) = year(cast(DATEADD(MONTH, -3, GETDATE()) as date)) and month(datefromparts(FISCPRD, PERIOD, 1)) = month(cast(DATEADD(MONTH, -3, GETDATE()) as date)) and PERIOD <> 0 group by MTRL) mtrbalsheetG3 on mtrbalsheetG3.MTRL = sysx.MTRL
left join (select sum(SALQTY) SALQTY, MTRL from [dbo].MTRBALSHEET where company = 1000 and year(datefromparts(FISCPRD, PERIOD, 1)) = year(cast(DATEADD(MONTH, -2, GETDATE()) as date)) and month(datefromparts(FISCPRD, PERIOD, 1)) = month(cast(DATEADD(MONTH, -2, GETDATE()) as date)) and PERIOD <> 0 group by MTRL) mtrbalsheetG2 on mtrbalsheetG2.MTRL = sysx.MTRL
left join (select sum(SALQTY) SALQTY, MTRL from [dbo].MTRBALSHEET where company = 1000 and year(datefromparts(FISCPRD, PERIOD, 1)) = year(cast(DATEADD(MONTH, -1, GETDATE()) as date)) and month(datefromparts(FISCPRD, PERIOD, 1)) = month(cast(DATEADD(MONTH, -1, GETDATE()) as date)) and PERIOD <> 0 group by MTRL) mtrbalsheetG1 on mtrbalsheetG1.MTRL = sysx.MTRL
left join (select sum(SALQTY) SALQTY, MTRL from [dbo].MTRBALSHEET where company = 1000 and year(datefromparts(FISCPRD, PERIOD, 1)) = year(GETDATE()) and month(datefromparts(FISCPRD, PERIOD, 1)) = month(GETDATE()) and PERIOD <> 0 group by MTRL) mtrbalsheetG on mtrbalsheetG.MTRL = sysx.MTRL
WHERE  A.company = 1000
       AND A.sosource = ((F.SODTYPE*100)+51)  
       AND A.TFPRMS   = 201
       AND A.fprms = 7021
       AND A.ISCANCEL =	0 
       AND A.ORIGIN	 <> 6
       AND A.APPRV   = 1
       AND C.PENDING  = 1  
       AND C.COMPANY  = 1000 
       AND C.RESTMODE IS NOT NULL  
       AND Isnull(C.qty1, 0) >= 1
       and year(A.trndate) = @YEAR
       and Isnull(C.lineval, 0) > 2500

--select p.* from #pwlhseis p

--Sunallassomenoi->Pelates->Epvnumia, apo: *IT SALES*->Sxetikes Ergasies-> Ekkremeis Paraggelies

-- Final export projection: calculates export vs retail stock comparisons,
-- renames columns to English-friendly labels and includes FIFO purchase prices.
select "Ημερ/νία"
        , "Παραστατικό"
        , "Α.Χ."
        , "Υποκ/μα"
        , "Χώρα"
        , "Επωνυμία"
        , "Κατάσταση"
        , "Κωδικός Ε."                  as 'Κωδικός EXP'
        , "Περιγραφή"
        , isnull("PART NUMBER", '')     as 'PART NUMBER'
        , "Εμπορ.κατηγορία"
        , "Κατασκευαστής"
        , "Ποσ.1"                       as 'Qty Sold'
        , "Τιμή Ευρώ"                   as 'Τιμή Πωλησης €'
        , "Τιμή Δολάριο"                as 'Τιμή Πωλησης $'
        , "Εκπτ.%1"
        , "Αξία"
        , "FIFO Αγοράς"                 as 'FIFO Αγοράς €'
        , StockPapada.UnitsInStock           as 'EXP Stock'
        , AnamenomenaPapada.Anamenomena       as 'ΕΧP Aναμεν.'
        , DesmeumenaPapada.CustomerOrderUnits    as 'ΕΧP Δεσμ.'
        , (StockPapada.UnitsInStock + StockSkroutz.UnitsInStock + StockDSV.UnitsInStock + StockTQL.UnitsInStock + StockDreamCargo.UnitsInStock + AnamenomenaPapada.Anamenomena)
            - (DesmeumenaPapada.CustomerOrderUnits + DesmeumenaSkroutz.CustomerOrderUnits + DesmeumenaDSV.CustomerOrderUnits + DesmeumenaTQL.CustomerOrderUnits + DesmeumenaDreamCargo.CustomerOrderUnits) as 'Actual Export Stock'
        , "Retail MG ID"                as 'Retail ID'
        , isnull(cast(RStockPapada.UnitsInStock as varchar), '')         as 'Retail Stock'
        , isnull(cast(RAnamenomenaPapada.Anamenomena as varchar), '')     as 'Retail Incoming'
        , isnull(cast(RDesmeumenaPapada.CustomerOrderUnits as varchar),'')   as 'Retail Δεσμ.'
        , (RStockPapada.UnitsInStock + RStockSkroutz.UnitsInStock + RStockDSV.UnitsInStock + RStockTQL.UnitsInStock + RStockDreamCargo.UnitsInStock + RAnamenomenaPapada.Anamenomena)
            - (RDesmeumenaPapada.CustomerOrderUnits + RDesmeumenaSkroutz.CustomerOrderUnits + RDesmeumenaDSV.CustomerOrderUnits + RDesmeumenaTQL.CustomerOrderUnits + RDesmeumenaDreamCargo.CustomerOrderUnits) as 'Actual Retail Stock'
        , "Net Retail Price"            as 'Net Retail'
        , "Month -3"                    as 'Three Months Ago'
        , "Month -2"                    as 'Two Months Ago'
        , "Month -1"                    as 'One Month Ago'
        , "This Month"
        , "Προμηθευτές"
from #pwlhseis p
left join (select COALESCE(SUM(CASE WHEN supplierid IN (13, 19, 20, 39) THEN unitsinstock ELSE 0 END), 0) 'UnitsInStock', ProductID from [magicom_shop_2019].[dbo].[Tbl_Products_Suppliers] GROUP BY productID) StockPapada on cast(StockPapada.ProductID  as varchar) = "Κωδικός Ε."
left join (select unitsinstock 'Anamenomena', ProductID from [magicom_shop_2019].[dbo].[Tbl_Products_Suppliers] where SupplierID = 18) AnamenomenaPapada on cast(AnamenomenaPapada.ProductID  as varchar) = "Κωδικός Ε."
left join (select COALESCE(SUM(CASE WHEN supplierid IN (13, 19, 20, 39) THEN CustomerOrderUnits ELSE 0 END), 0) 'CustomerOrderUnits', ProductID from [magicom_shop_2019].[dbo].[Tbl_Products_Suppliers] GROUP BY productID) DesmeumenaPapada on cast(DesmeumenaPapada.ProductID  as varchar) = "Κωδικός Ε."

left join (select COALESCE(SUM(CASE WHEN supplierid IN (13, 19, 20, 39) THEN unitsinstock ELSE 0 END), 0) 'UnitsInStock', ProductID from [magicom_shop_2019].[dbo].[Tbl_Products_Suppliers] GROUP BY productID) RStockPapada on cast(RStockPapada.ProductID  as varchar) = "Retail MG ID"
left join (select unitsinstock 'Anamenomena', ProductID from [magicom_shop_2019].[dbo].[Tbl_Products_Suppliers] where SupplierID = 18) RAnamenomenaPapada on cast(RAnamenomenaPapada.ProductID  as varchar) = "Retail MG ID"
left join (select COALESCE(SUM(CASE WHEN supplierid IN (13, 19, 20, 39) THEN CustomerOrderUnits ELSE 0 END), 0) 'CustomerOrderUnits', ProductID from [magicom_shop_2019].[dbo].[Tbl_Products_Suppliers] GROUP BY productID) RDesmeumenaPapada on cast(RDesmeumenaPapada.ProductID  as varchar) = "Retail MG ID"

left join (select COALESCE(SUM(CASE WHEN supplierid IN (42) THEN unitsinstock ELSE 0 END), 0) 'UnitsInStock', ProductID from [magicom_shop_2019].[dbo].[Tbl_Products_Suppliers] GROUP BY productID) StockSkroutz on cast(StockSkroutz.ProductID  as varchar) = "Κωδικός Ε."
--left join (select SupplierOrderUnits 'Anamenomena', ProductID from [magicom_shop_2019].[dbo].[Tbl_Products_Suppliers] where SupplierID = 42) AnamenomenaSkroutz on cast(AnamenomenaSkroutz.ProductID  as varchar) = "Κωδικός Ε."
left join (select COALESCE(SUM(CASE WHEN supplierid IN (42) THEN CustomerOrderUnits ELSE 0 END), 0) 'CustomerOrderUnits', ProductID from [magicom_shop_2019].[dbo].[Tbl_Products_Suppliers] GROUP BY productID) DesmeumenaSkroutz on cast(DesmeumenaSkroutz.ProductID  as varchar) = "Κωδικός Ε."

left join (select COALESCE(SUM(CASE WHEN supplierid IN (42) THEN unitsinstock ELSE 0 END), 0) 'UnitsInStock', ProductID from [magicom_shop_2019].[dbo].[Tbl_Products_Suppliers] GROUP BY productID) RStockSkroutz on cast(RStockSkroutz.ProductID  as varchar) = "Retail MG ID"
--left join (select SupplierOrderUnits 'Anamenomena', ProductID from [magicom_shop_2019].[dbo].[Tbl_Products_Suppliers] where SupplierID = 42) RAnamenomenaSkroutz on cast(RAnamenomenaSkroutz.ProductID  as varchar) = "Retail MG ID"
left join (select COALESCE(SUM(CASE WHEN supplierid IN (42) THEN CustomerOrderUnits ELSE 0 END), 0) 'CustomerOrderUnits', ProductID from [magicom_shop_2019].[dbo].[Tbl_Products_Suppliers] GROUP BY productID) RDesmeumenaSkroutz on cast(RDesmeumenaSkroutz.ProductID  as varchar) = "Retail MG ID"

left join (select COALESCE(SUM(CASE WHEN supplierid IN (43) THEN unitsinstock ELSE 0 END), 0) 'UnitsInStock', ProductID from [magicom_shop_2019].[dbo].[Tbl_Products_Suppliers] GROUP BY productID) StockDSV on cast(StockDSV.ProductID  as varchar) = "Κωδικός Ε."
--left join (select SupplierOrderUnits 'Anamenomena', ProductID from [magicom_shop_2019].[dbo].[Tbl_Products_Suppliers] where SupplierID = 43) AnamenomenaDSV on cast(AnamenomenaDSV.ProductID  as varchar) = "Κωδικός Ε."
left join (select COALESCE(SUM(CASE WHEN supplierid IN (43) THEN CustomerOrderUnits ELSE 0 END), 0) 'CustomerOrderUnits', ProductID from [magicom_shop_2019].[dbo].[Tbl_Products_Suppliers] GROUP BY productID) DesmeumenaDSV on cast(DesmeumenaDSV.ProductID  as varchar) = "Κωδικός Ε."

left join (select COALESCE(SUM(CASE WHEN supplierid IN (43) THEN unitsinstock ELSE 0 END), 0) 'UnitsInStock', ProductID from [magicom_shop_2019].[dbo].[Tbl_Products_Suppliers] GROUP BY productID) RStockDSV on cast(RStockDSV.ProductID  as varchar) = "Retail MG ID"
--left join (select SupplierOrderUnits 'Anamenomena', ProductID from [magicom_shop_2019].[dbo].[Tbl_Products_Suppliers] where SupplierID = 43) RAAnamenomenaDSV on cast(RAAnamenomenaDSV.ProductID  as varchar) = "Retail MG ID"
left join (select COALESCE(SUM(CASE WHEN supplierid IN (43) THEN CustomerOrderUnits ELSE 0 END), 0) 'CustomerOrderUnits', ProductID from [magicom_shop_2019].[dbo].[Tbl_Products_Suppliers] GROUP BY productID) RDesmeumenaDSV on cast(RDesmeumenaDSV.ProductID  as varchar) = "Retail MG ID"

left join (select COALESCE(SUM(CASE WHEN supplierid IN (41) THEN unitsinstock ELSE 0 END), 0) 'UnitsInStock', ProductID from [magicom_shop_2019].[dbo].[Tbl_Products_Suppliers] GROUP BY productID) StockTQL on cast(StockTQL.ProductID  as varchar) = "Κωδικός Ε."
--left join (select unitsinstock 'Anamenomena', ProductID from [magicom_shop_2019].[dbo].[Tbl_Products_Suppliers] where SupplierID = 41) AnamenomenaTQL on cast(AnamenomenaTQL.ProductID  as varchar) = "Κωδικός Ε."
left join (select COALESCE(SUM(CASE WHEN supplierid IN (41) THEN CustomerOrderUnits ELSE 0 END), 0) 'CustomerOrderUnits', ProductID from [magicom_shop_2019].[dbo].[Tbl_Products_Suppliers] GROUP BY productID) DesmeumenaTQL on cast(DesmeumenaTQL.ProductID  as varchar) = "Κωδικός Ε."

left join (select COALESCE(SUM(CASE WHEN supplierid IN (41) THEN unitsinstock ELSE 0 END), 0) 'UnitsInStock', ProductID from [magicom_shop_2019].[dbo].[Tbl_Products_Suppliers] GROUP BY productID) RStockTQL on cast(RStockTQL.ProductID  as varchar) = "Retail MG ID"
--left join (select unitsinstock 'Anamenomena', ProductID from [magicom_shop_2019].[dbo].[Tbl_Products_Suppliers] where SupplierID = 41) RAnamenomenaTQL on cast(RAnamenomenaTQL.ProductID  as varchar) = "Retail MG ID"
left join (select COALESCE(SUM(CASE WHEN supplierid IN (41) THEN CustomerOrderUnits ELSE 0 END), 0) 'CustomerOrderUnits', ProductID from [magicom_shop_2019].[dbo].[Tbl_Products_Suppliers] GROUP BY productID) RDesmeumenaTQL on cast(RDesmeumenaTQL.ProductID  as varchar) = "Retail MG ID"

left join (select COALESCE(SUM(CASE WHEN supplierid IN (22) THEN unitsinstock ELSE 0 END), 0) 'UnitsInStock', ProductID from [magicom_shop_2019].[dbo].[Tbl_Products_Suppliers] GROUP BY productID) StockDreamCargo on cast(StockDreamCargo.ProductID  as varchar) = "Κωδικός Ε."
--left join (select unitsinstock 'Anamenomena', ProductID from [magicom_shop_2019].[dbo].[Tbl_Products_Suppliers] where SupplierID = 41) AnamenomenaDreamCargo on cast(AnamenomenaDreamCargo.ProductID  as varchar) = "Κωδικός Ε."
left join (select COALESCE(SUM(CASE WHEN supplierid IN (22) THEN CustomerOrderUnits ELSE 0 END), 0) 'CustomerOrderUnits', ProductID from [magicom_shop_2019].[dbo].[Tbl_Products_Suppliers] GROUP BY productID) DesmeumenaDreamCargo on cast(DesmeumenaDreamCargo.ProductID  as varchar) = "Κωδικός Ε."

left join (select COALESCE(SUM(CASE WHEN supplierid IN (22) THEN unitsinstock ELSE 0 END), 0) 'UnitsInStock', ProductID from [magicom_shop_2019].[dbo].[Tbl_Products_Suppliers] GROUP BY productID) RStockDreamCargo on cast(RStockDreamCargo.ProductID  as varchar) = "Retail MG ID"
--left join (select unitsinstock 'Anamenomena', ProductID from [magicom_shop_2019].[dbo].[Tbl_Products_Suppliers] where SupplierID = 41) RAnamenomenaDreamCargo on cast(RAnamenomenaDreamCargo.ProductID  as varchar) = "Retail MG ID"
left join (select COALESCE(SUM(CASE WHEN supplierid IN (22) THEN CustomerOrderUnits ELSE 0 END), 0) 'CustomerOrderUnits', ProductID from [magicom_shop_2019].[dbo].[Tbl_Products_Suppliers] GROUP BY productID) RDesmeumenaDreamCargo on cast(RDesmeumenaDreamCargo.ProductID  as varchar) = "Retail MG ID"

drop table #pwlhseis
drop table #agores

END
