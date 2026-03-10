/*
       File: B2BSpecialDealsNikonas1.sql
       Purpose: Stored procedure that aggregates B2B special-deals, inventory
                                    and ordering information across multiple warehouses and sources.
                                    Produces temporary tables (#agores, #pwlhseis, #endodiakinisi, #main_table)
                                    and a final combined result for reporting and analysis.
       Notes: Alters procedure [dbo].[B2BSpecialDealsNikonas1]. No input parameters.
       Last modified: 2026-03-09
*/
USE [soft1]
GO
/****** Object:  StoredProcedure [dbo].[B2BSpecialDealsNikonas1]    Script Date: 3/9/2026 1:21:04 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [dbo].[B2BSpecialDealsNikonas1] 
AS
BEGIN
SET NOCOUNT ON;

/*
       Temp table: #agores
       - Purpose: capture purchase/order lines (supplier invoices, packing lists)
              with item metadata (category, manufacturer, currency) used later to
              compute expected arrivals, weighted average arrival price and supplier lists.
       - Key filters applied in the source SELECT: company=1000, sosource=1251, sodtype=12
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
       - Purpose: collects recent sales/order lines used to compute Actual30/90DaysSales
              and to join sales activity back to products for demand metrics.
       - Key filters: company=1000, sosource=1351, soredir=0, sodtype=13
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
       A.fprms                                          as 'FPRMS'
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
 left join (select branch, NAME from branch where company = 1000) branch on branch.branch = A.branch
WHERE  A.company = 1000
       AND A.sosource = 1351
       AND A.soredir = 0
       AND A.sodtype = 13
       --AND Isnull(C.qty1, 0) >= 1
       and a.fprms in (7061,7062,7063,7064,7066,7067,7068,7070,7071,7072,7073,7074,7075,7076,7077,7078,7079,7080,7082,7094,7095,7127,7162,7163,7201,7203,7205,7207,7209,7210,7211,7213,7297,22111)
       --and datediff(day, A.trndate, GETDATE()) between 0 and 90

/*
       Temp table: #endodiakinisi
       - Purpose: tracks internal transfers / internal receipts relevant to expected arrivals
              for items (used to adjust available quantities and expected inflows).
       - Key filters: company=1000, sosource=1151 and FPRMS for internal transfer types.
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
        whouse.NAME           as 'Α.Χ.'
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
WHERE A.COMPANY=1000 AND A.SOSOURCE=1151 AND A.FPRMS=2500 and A.FULLYTRANSF IN (0,2)

/*
       Temp table: #main_table
       - Purpose: aggregated item-level reporting table combining stock, supplier
              incoming quantities, expected arrivals, sales history and cost computations.
       - Notes: joins multiple external supplier tables (magicom_shop_2019) and
              computes metrics like 'Ready Stock', 'Actual Costs', 'FIFO' and 'Final Stock'.
*/
drop table if exists #main_table
-- Main aggregation: compute item-level metrics and join supplier/stock sources.
-- Important computed metrics:
--   'Ready Stock'  = sum of on-hand fields across multiple systems (P, TQL, DC, DSV)
--   'Actual Costs'  = weighted average cost using FIFO for existing stock and
--                     weighted arrival prices for expected inbound quantities
--   'Final Stock'   = supplier incoming + available stock - reserved quantities
-- The temporary aggregates from #agores and #pwlhseis are joined below
-- to compute expected arrivals and recent sales used in recommendations.
SELECT A.code,
       A.NAME,
       isnull(A.CATNAME, '')                                        as 'Κατηγορία',
       isnull(A.MANNAME, '')                                        as 'Κατασκευαστής',
       isnull(A.Punitsinstock, 0) + isnull(A.unitsinstock, 0) + isnull(A.DCunitsinstock, 0) + isnull(A.DSVunitsinstock, 0) as 'Ready Stock',
       isnull(A.PCustomerOrderUnits, 0) + isnull(A.SupplierOrderUnits, 0) + isnull(A.DCCustomerOrderUnits, 0) + isnull(A.DSVCustomerOrderUnits, 0) as 'Des',
       isnull(A.Punitsinstock, 0) - isnull(EikonikaAnamenomena.Anamenomena, 0)  as 'MG Απόθεμα',
       isnull(AnamonesPYP.[Ποσ.1], 0)                               as 'MG Αναμ.',
       isnull(A.PCustomerOrderUnits, 0)                             as 'MG Δεσμ.',
       isnull(A.unitsinstock, 0)                                    as 'TQL',
       isnull(AnamonesTQL.[Ποσ.1], 0)                               as 'Αναμ. TQL',
       isnull(A.SupplierOrderUnits, 0)                              as 'TQL Δεσμ.',
       isnull(A.DCunitsinstock, 0)                                  as 'Dream Cargo',
       isnull(AnamonesDC.[Ποσ.1] , 0)                               as 'Αναμ. DC',
       isnull(A.DCCustomerOrderUnits, 0)                            as 'DC Δεσμ.',
       isnull(A.DSVunitsinstock, 0)                                 as 'DSV', 
       isnull(AnamonesDSV.[Ποσ.1], 0)                               as 'Αναμ. DSV',
       --isnull(cast((case when 'ID'+cast(A.ProductID as varchar) = cast(A.SupplierProductID as varchar) then cast((A.DSVSupplierOrderUnits - isnull(A.QTY1, 0)) as varchar) else A.DSVSupplierOrderUnits end)  as varchar), 0.00) as 'Αν.DSV',
       isnull(A.DSVCustomerOrderUnits, 0)                           as 'DSV Δεσμ.',
       (isnull(A.Punitsinstock, 0) + isnull(A.unitsinstock, 0) + isnull(A.DCunitsinstock, 0) + isnull(A.DSVunitsinstock, 0)) - (isnull(A.PCustomerOrderUnits, 0) + isnull(A.SupplierOrderUnits, 0) + isnull(A.DCCustomerOrderUnits, 0) + isnull(A.DSVCustomerOrderUnits, 0))                                                                                 as 'Ready Stock-Δεσμευμένα',
       isnull(A.PSupplierOrderUnits, 0)                             as 'Incoming stock',
       NULL                                                         as 'Actual Stock',
       ((isnull(A.Punitsinstock, 0) + isnull(A.unitsinstock, 0) + isnull(A.DCunitsinstock, 0) + isnull(A.DSVunitsinstock, 0)) + isnull(A.PSupplierOrderUnits, 0) - (isnull(A.PCustomerOrderUnits, 0) + isnull(A.SupplierOrderUnits, 0) + isnull(A.DCCustomerOrderUnits, 0) + isnull(A.DSVCustomerOrderUnits, 0) + isnull(Anamones_EXP_RET.[Ποσ.1], 0)))      as 'Actual Stock B2B',
       EikonikaAnamenomena.Anamenomena as 'Εικονικά Αναμενόμενα',      
       cast(isnull(A.PSupplierOrderUnits, 0) + ((isnull(A.Punitsinstock, 0) - isnull(EikonikaAnamenomena.Anamenomena, 0)) + isnull(A.unitsinstock, 0) + isnull(A.DCunitsinstock, 0) + isnull(A.DSVunitsinstock, 0)) - (isnull(A.PCustomerOrderUnits, 0) + isnull(A.SupplierOrderUnits, 0) + isnull(A.DCCustomerOrderUnits, 0) + isnull(A.DSVCustomerOrderUnits, 0)) as varchar)                             as 'Final Stock',
       isnull(Actual90DaysSales.[Ποσ.1], 0)                         as 'Actual 90 Days Sales',
       isnull(Actual30DaysSales.[Ποσ.1], 0)                         as 'Actual 30 Days Sales',
       isnull(A.cccpartnoexp, '')                                   as 'Part No Export',
       isnull(A.CCCBARCODETP, '')                                   as 'Specs',
       isnull(D.CODE, '')                                           as 'Κωδικός',
       isnull(apothema.Apothema, 0)                                 as 'Απόθεμα Σχετικού Είδους',
       isnull(anamenomena.Anamenomena, 0)                           as 'Αναμενόμενα Σχετικού Είδους',
       isnull(desmeumena.Desmeumena, 0)                             as 'Δεσμευμένα Σχετικού Είδους',
       isnull(apothema.Apothema, 0) + isnull(anamenomena.Anamenomena, 0) - isnull(desmeumena.Desmeumena, 0) as 'ACTUAL RETAIL',
       (case when isnull(AnamonesPYP.Weighted_Average, 0) + isnull(AnamonesTQL.Weighted_Average, 0) + isnull(AnamonesDSV.Weighted_Average, 0) + isnull(AnamonesDC.Weighted_Average, 0) = 0 then PKOSTOS.PKOSTOS else isnull(AnamonesPYP.Weighted_Average, 0) + isnull(AnamonesTQL.Weighted_Average, 0) + isnull(AnamonesDSV.Weighted_Average, 0) + isnull(AnamonesDC.Weighted_Average, 0) end)  as 'πρότυπο κόστος',
       --isnull(AnamonesPYP.Weighted_Average, 0) + isnull(AnamonesTQL.Weighted_Average, 0) + isnull(AnamonesDSV.Weighted_Average, 0) + isnull(AnamonesDC.Weighted_Average, 0) as 'asd',
       FIFO.FIFO                                                    as 'FIFO Price',
       (isnull(((case when (isnull(A.Punitsinstock, 0) - isnull(EikonikaAnamenomena.Anamenomena, 0)) < 0 then 0 else (isnull(A.Punitsinstock, 0) - isnull(EikonikaAnamenomena.Anamenomena, 0)) end) + isnull(A.unitsinstock, 0) + isnull(A.DCunitsinstock, 0) + isnull(A.DSVunitsinstock, 0)) *  FIFO.FIFO, 0) 
                 +  isnull((isnull(AnamonesPYP.[Ποσ.1], 0) + isnull(AnamonesTQL.[Ποσ.1], 0) + isnull(AnamonesDSV.[Ποσ.1], 0) + isnull(AnamonesDC.[Ποσ.1], 0)) * (isnull(AnamonesPYP.Weighted_Average, 0) + isnull(AnamonesTQL.Weighted_Average, 0) + isnull(AnamonesDSV.Weighted_Average, 0) + isnull(AnamonesDC.Weighted_Average, 0)), 0)) 
                 / ((case when (isnull(A.Punitsinstock, 0) - isnull(EikonikaAnamenomena.Anamenomena, 0)) < 0 then 0 else (isnull(A.Punitsinstock, 0) - isnull(EikonikaAnamenomena.Anamenomena, 0)) end) + isnull(A.unitsinstock, 0) + isnull(A.DCunitsinstock, 0) + isnull(A.DSVunitsinstock, 0) + isnull(AnamonesPYP.[Ποσ.1], 0) + isnull(AnamonesTQL.[Ποσ.1], 0) + isnull(AnamonesDSV.[Ποσ.1], 0) + isnull(AnamonesDC.[Ποσ.1], 0)) as 'Actual Costs',
       D.PRICER                                                     as 'Net Retail τιμή',
       isnull(ag.Promitheutes, '')                                 as 'Προμηθευτές'
into #main_table 
FROM   ((select A.*, mtrcategoryEXP.NAME as CATNAME
                , mtrmanfctr.NAME   as MANNAME
                , TQL.unitsinstock
                --, ATQL.CustomerOrderUnits
                , DTQL.SupplierOrderUnits
                , Papada.unitsinstock 'Punitsinstock'
                , DPapada.CustomerOrderUnits 'PCustomerOrderUnits'
                , APapada.unitsinstock 'PSupplierOrderUnits'
                , DSV.unitsinstock 'DSVunitsinstock'
                --, ADSV.SupplierOrderUnits 'DSVSupplierOrderUnits'
                , DDSV.Desmeumena 'DSVCustomerOrderUnits'
                , StockDreamCargo.unitsinstock 'DCunitsinstock'
                --, AnamenomenaDreamCargo.SupplierOrderUnits 'DCSupplierOrderUnits'
                , DesmeumenaDreamCargo.CustomerOrderUnits 'DCCustomerOrderUnits'
            from mtrl A 
            join (select MTRCATEGORY, name from [dbo].[MTRCATEGORY] where company = 1000 AND isactive = 1 AND sodtype = 51 and name like '%EXP%') mtrcategoryEXP on mtrcategoryEXP.MTRCATEGORY = A.MTRCATEGORY
            left join (select * from [dbo].[MTRMANFCTR] where company = 1000 and isactive = 1) mtrmanfctr on mtrmanfctr.MTRMANFCTR = A.MTRMANFCTR
            left join (select sum(unitsinstock) 'unitsinstock', ProductID from [magicom_shop_2019].[dbo].[Tbl_Products_Suppliers] where supplierid IN (13, 19, 20, 39) group by ProductID) Papada on cast(Papada.ProductID  as varchar) = A.CODE
            left join (select sum(CustomerOrderUnits) 'CustomerOrderUnits', ProductID from [magicom_shop_2019].[dbo].[Tbl_Products_Suppliers] where supplierid IN (13, 19, 20, 39) group by ProductID) DPapada on cast(DPapada.ProductID  as varchar) = A.CODE
            left join (select unitsinstock, ProductID from [magicom_shop_2019].[dbo].[Tbl_Products_Suppliers] where supplierid = 18) APapada on cast(APapada.ProductID  as varchar) = A.CODE  COLLATE SQL_Latin1_General_CP1253_CI_AI
            left join (select unitsinstock, ProductID from [magicom_shop_2019].[dbo].[Tbl_Products_Suppliers] where supplierid = 41) TQL on cast(TQL.ProductID  as varchar) = A.CODE
            --left join (select CustomerOrderUnits, ProductID from [magicom_shop_2019].[dbo].[Tbl_Products_Suppliers] where supplierid = 41) ATQL on cast(ATQL.ProductID  as varchar) = A.CODE
            left join (select SupplierOrderUnits, ProductID from [magicom_shop_2019].[dbo].[Tbl_Products_Suppliers] where supplierid = 41) DTQL on cast(DTQL.ProductID  as varchar) = A.CODE
            left join (select unitsinstock, ProductID from [magicom_shop_2019].[dbo].[Tbl_Products_Suppliers] where supplierid = 43) DSV on cast(DSV.ProductID  as varchar) = A.CODE
            left join (select sum(CustomerOrderUnits) 'Desmeumena', ProductID from [magicom_shop_2019].[dbo].[Tbl_Products_Suppliers] where supplierid = 43 group by ProductID) DDSV on cast(DDSV.ProductID  as varchar) = A.CODE
            --left join (select unitsinstock 'SupplierOrderUnits', SupplierProductID from [magicom_shop_2019].[dbo].[Tbl_Products_Suppliers] where supplierid = 18) ADSV on cast(ADSV.SupplierProductID  as varchar) = 'ID' + A.CODE COLLATE SQL_Latin1_General_CP1253_CI_AI
            left join (select COALESCE(SUM(CASE WHEN supplierid IN (22) THEN unitsinstock ELSE 0 END), 0) 'UnitsInStock', ProductID from [magicom_shop_2019].[dbo].[Tbl_Products_Suppliers] GROUP BY productID) StockDreamCargo on cast(StockDreamCargo.ProductID  as varchar) = A.CODE
            --left join (select SupplierOrderUnits, ProductID from [magicom_shop_2019].[dbo].[Tbl_Products_Suppliers] where SupplierID = 22) AnamenomenaDreamCargo on cast(AnamenomenaDreamCargo.ProductID  as varchar) = A.CODE
            left join (select COALESCE(SUM(CASE WHEN supplierid IN (22) THEN CustomerOrderUnits ELSE 0 END), 0) 'CustomerOrderUnits', ProductID from [magicom_shop_2019].[dbo].[Tbl_Products_Suppliers] GROUP BY productID) DesmeumenaDreamCargo on cast(DesmeumenaDreamCargo.ProductID  as varchar) = A.CODE) A
       left join (select ag.mtrl
                            , STUFF((select Distinct ', ' + Prom from #agores ag2 where ag2.Description = ag.Description and "Prom" not in ('TOTAL QUALITY LOGISTICS ΑΕ', 'DREAM CARGO SERVICES SRL' , 'DSV AIR & SEA ΜΟΝΟΠΡΟΣΩΠΗ ΑΝΩΝΥΜΗ ΕΤΑΙΡΕΙΑ', 'DSV AIR &amp; SEA ΜΟΝΟΠΡΟΣΩΠΗ ΑΝΩΝΥΜΗ ΕΤΑΙΡΕΙΑ') FOR XML PATH ('')),1,2,'') AS 'Promitheutes'
                    from #agores ag
                    where "Prom" not in ('TOTAL QUALITY LOGISTICS ΑΕ', 'DREAM CARGO SERVICES SRL' , 'DSV AIR & SEA ΜΟΝΟΠΡΟΣΩΠΗ ΑΝΩΝΥΜΗ ΕΤΑΙΡΕΙΑ', 'DSV AIR &amp; SEA ΜΟΝΟΠΡΟΣΩΠΗ ΑΝΩΝΥΜΗ ΕΤΑΙΡΕΙΑ') 
                    group by Description, ag.mtrl, "Κωδικός Ε.", "Εμπορ.κατηγορία") ag on ag.mtrl = A.mtrl
       left join mtrdata B ON B.company = 1000 AND A.mtrl = B.mtrl AND B.fiscprd = 2025)
       left join mtrl D ON A.relitem = D.mtrl
       left join (select p.[Κωδικός Ε.], sum(p.[Ποσ.1]) as [Ποσ.1] from #pwlhseis p where datediff(day, "Ημερ/νία", GETDATE()) between 0 and 90 group by p.[Κωδικός Ε.] ) Actual90DaysSales on Actual90DaysSales.[Κωδικός Ε.] = A.CODE
       left join (select p.[Κωδικός Ε.], sum(p.[Ποσ.1]) as [Ποσ.1] from #pwlhseis p where datediff(day, "Ημερ/νία", GETDATE()) between 0 and 30 group by p.[Κωδικός Ε.]) Actual30DaysSales on Actual30DaysSales.[Κωδικός Ε.] = D.CODE
       
       left join (select p.[Κωδικός Ε.], sum(UNCOVQTY) as [Ποσ.1], sum("Τιμή Ευρώ"*UNCOVQTY)/sum(UNCOVQTY) as Weighted_Average from #agores p where "Α.Χ." in ('Κεντρικός Παπαδά', 'Κεντρικός Σολωμού', 'Κεντρικός Πειραιά', 'Κεντρικός Βουλιαγμένη') and "Τύπος" in ('Παραγγελία Σε Προμηθευτή', 'Packing list προμηθευτή') and UNCOVQTY <> 0 group by p.[Κωδικός Ε.]) AnamonesPYP on AnamonesPYP.[Κωδικός Ε.] = A.CODE
       left join (select p.[Κωδικός Ε.], sum(UNCOVQTY) as [Ποσ.1], sum("Τιμή Ευρώ"*UNCOVQTY)/sum(UNCOVQTY) as Weighted_Average from #agores p where "Α.Χ." = 'Total Quality Logistic' and "Τύπος" in ('Παραγγελία Σε Προμηθευτή', 'Packing list προμηθευτή') and UNCOVQTY <> 0 group by p.[Κωδικός Ε.]) AnamonesTQL on AnamonesTQL.[Κωδικός Ε.] = A.CODE
       left join (select p.[Κωδικός Ε.], sum(UNCOVQTY) as [Ποσ.1], sum("Τιμή Ευρώ"*UNCOVQTY)/sum(UNCOVQTY) as Weighted_Average from #agores p where "Α.Χ." = 'DSV' and "Τύπος" in ('Παραγγελία Σε Προμηθευτή', 'Packing list προμηθευτή') and UNCOVQTY <> 0 group by p.[Κωδικός Ε.]) AnamonesDSV on AnamonesDSV.[Κωδικός Ε.] = A.CODE
       left join (select p.[Κωδικός Ε.], sum(UNCOVQTY) as [Ποσ.1], sum("Τιμή Ευρώ"*UNCOVQTY)/sum(UNCOVQTY) as Weighted_Average from #agores p where "Α.Χ." = 'Dream Cargo' and "Τύπος" in ('Παραγγελία Σε Προμηθευτή', 'Packing list προμηθευτή') and UNCOVQTY <> 0 group by p.[Κωδικός Ε.]) AnamonesDC on AnamonesDC.[Κωδικός Ε.] = A.CODE
       
       left join (select sum(Amount) as [Ποσ.1], [Κωδικός Ε.] from #endodiakinisi group by [Κωδικός Ε.]) Anamones_EXP_RET on Anamones_EXP_RET.[Κωδικός Ε.] = A.CODE

       left join (select p.[Κωδικός Ε.], sum(UNCOVQTY) as 'Anamenomena' from #agores p where "Α.Χ." in ('Κεντρικός Σολωμού', 'Κεντρικός Παπαδά', 'Κεντρικός Πειραιά', 'Κεντρικός Βουλιαγμένη') and UNCOVQTY <> 0 and Prom in ('DSV AIR & SEA ΜΟΝΟΠΡΟΣΩΠΗ ΑΝΩΝΥΜΗ ΕΤΑΙΡΕΙΑ', 'TOTAL QUALITY LOGISTICS ΑΕ') and "Τύπος" in ('Παραγγελία Σε Προμηθευτή', 'Packing list προμηθευτή') group by p.[Κωδικός Ε.]) EikonikaAnamenomena on EikonikaAnamenomena.[Κωδικός Ε.] = D.CODE

       --left join (select sum(unitsinstock) 'Apothema', SupplierProductID from [magicom_shop_2019].[dbo].[Tbl_Products_Suppliers] where SupplierID in (13, 19, 20, 39, 22, 41, 42, 43) group by SupplierProductID) apothema on apothema.SupplierProductID = D.CODE COLLATE SQL_Latin1_General_CP1253_CI_AI
       left join (select sum(unitsinstock) 'Apothema', SupplierProductID from [magicom_shop_2019].[dbo].[Tbl_Products_Suppliers] where SupplierID in (13, 19, 20, 39, 42) group by SupplierProductID) apothema on apothema.SupplierProductID = D.CODE COLLATE SQL_Latin1_General_CP1253_CI_AI
       left join (select unitsinstock 'Anamenomena', SupplierProductID from [magicom_shop_2019].[dbo].[Tbl_Products_Suppliers] where SupplierID = 18) anamenomena on anamenomena.SupplierProductID = 'ID'+D.CODE COLLATE SQL_Latin1_General_CP1253_CI_AI
       left join (select sum(CustomerOrderUnits) 'Desmeumena', SupplierProductID from [magicom_shop_2019].[dbo].[Tbl_Products_Suppliers] group by SupplierProductID) desmeumena on desmeumena.SupplierProductID = D.CODE COLLATE SQL_Latin1_General_CP1253_CI_AI
       left join (select sum(PURFIFO) 'FIFO', MTRL from MTRCPRICES where COMPANY=1000 AND FISCPRD=YEAR(GETDATE()) AND PERIOD=1000 group by MTRL) FIFO on FIFO.MTRL = A.MTRL
       left join (select sum(STANDCOST) 'PKOSTOS', MTRL from MTRCPRICES where COMPANY=1000 AND FISCPRD=YEAR(GETDATE()) AND PERIOD=1000 group by MTRL) PKOSTOS on PKOSTOS.MTRL = A.MTRL
WHERE  A.company = 1000
       and A.sodtype = 51
       and (((isnull(A.Punitsinstock, 0) - isnull(EikonikaAnamenomena.Anamenomena, 0)) + isnull(A.unitsinstock, 0) + isnull(A.DCunitsinstock, 0) + isnull(A.DSVunitsinstock, 0) + isnull(AnamonesPYP.[Ποσ.1], 0) + isnull(AnamonesTQL.[Ποσ.1], 0) + isnull(AnamonesDSV.[Ποσ.1], 0) + isnull(AnamonesDC.[Ποσ.1], 0)) > 1
                or (case when A.CATNAME like '%efurbi%' then (isnull(A.Punitsinstock, 0) - isnull(EikonikaAnamenomena.Anamenomena, 0)) + isnull(A.unitsinstock, 0) + isnull(A.DCunitsinstock, 0) + isnull(A.DSVunitsinstock, 0) + isnull(AnamonesPYP.[Ποσ.1], 0) + isnull(AnamonesTQL.[Ποσ.1], 0) + isnull(AnamonesDSV.[Ποσ.1], 0) + isnull(AnamonesDC.[Ποσ.1], 0) else 0 end) > 5)
-- Alternate result branch: handles a different category filter set
-- (laptops/smartphones/refurbished/outlet/used) and returns the same
-- metric schema but sources some values from alternate supplier joins.
union
SELECT A.code,
       A.NAME,
       isnull(A.CATNAME, '')                                        as 'Κατηγορία',
       isnull(A.MANNAME, '')                                        as 'Κατασκευαστής',
       isnull(A.Punitsinstock, 0) + isnull(A.unitsinstock, 0) + isnull(A.DCunitsinstock, 0) + isnull(A.DSVunitsinstock, 0) as 'Ready Stock',
       isnull(A.PCustomerOrderUnits, 0) + isnull(A.SupplierOrderUnits, 0) + isnull(A.DCCustomerOrderUnits, 0) + isnull(A.DSVCustomerOrderUnits, 0) as 'Des',
       isnull(A.Punitsinstock, 0)                                   as 'MG Απόθεμα',
       isnull(AnamonesPYP.[Ποσ.1] , 0)                              as 'MG Αναμ.',
       isnull(A.PCustomerOrderUnits, 0)                             as 'MG Δεσμ.',
       isnull(A.unitsinstock, 0)                                    as 'TQL',
       isnull(AnamonesTQL.[Ποσ.1], 0)                               as 'Αναμ. TQL',
       isnull(A.SupplierOrderUnits, 0)                              as 'TQL Δεσμ.',
       isnull(A.DCunitsinstock , 0)                                 as 'Dream Cargo',
       isnull(AnamonesDC.[Ποσ.1], 0)                                as 'Αναμ. DC',
       isnull(A.DCCustomerOrderUnits, 0)                            as 'DC Δεσμ.',
       isnull(A.DSVunitsinstock, 0)                                 as 'DSV', 
       isnull(AnamonesDSV.[Ποσ.1], 0)                               as 'Αναμ. DSV',
       --isnull(cast((case when 'ID'+cast(A.ProductID as varchar) = cast(A.SupplierProductID as varchar) then cast((A.DSVSupplierOrderUnits - isnull(A.QTY1, 0)) as varchar) else A.DSVSupplierOrderUnits end)  as varchar), 0.00) as 'Αν.DSV',
       isnull(A.DSVCustomerOrderUnits, 0)                           as 'DSV Δεσμ.',
       (isnull(A.Punitsinstock, 0) + isnull(A.unitsinstock, 0) + isnull(A.DCunitsinstock, 0) + isnull(A.DSVunitsinstock, 0)) - (isnull(A.PCustomerOrderUnits, 0) + isnull(A.SupplierOrderUnits, 0) + isnull(A.DCCustomerOrderUnits, 0) + isnull(A.DSVCustomerOrderUnits, 0)) as 'Ready Stock-Δεσμευμένα',
       isnull(A.PSupplierOrderUnits, 0)                             as 'Incoming stock',
       cast(isnull(A.PSupplierOrderUnits, 0) + (isnull(A.Punitsinstock, 0) + isnull(A.unitsinstock, 0) + isnull(A.DCunitsinstock, 0) + isnull(A.DSVunitsinstock, 0)) - (isnull(A.PCustomerOrderUnits, 0) + isnull(A.SupplierOrderUnits, 0) + isnull(A.DCCustomerOrderUnits, 0) + isnull(A.DSVCustomerOrderUnits, 0)) as varchar) as 'Actual Stock',
       NULL                                                         as 'Actual Stock B2B',
       NULL                                                         as 'Εικονικά Αναμενόμενα',  
       cast(isnull(A.PSupplierOrderUnits, 0) as varchar)            as 'Final Stock',
       isnull(Actual90DaysSales.[Ποσ.1], 0)                         as 'Actual 90 Days Sales',
       isnull(Actual30DaysSales.[Ποσ.1], 0)                         as 'Actual 30 Days Sales',
       isnull(A.cccpartnoexp, '')                                   as 'Part No Export',
       isnull(A.CCCBARCODETP, '')                                   as 'Specs',
       isnull(D.CODE, '')                                           as 'Κωδικός',
       isnull(apothema.Apothema, 0)                                 as 'Απόθεμα Σχετικού Είδους',
       isnull(anamenomena.Anamenomena, 0)                           as 'Αναμενόμενα Σχετικού Είδους',
       isnull(desmeumena.Desmeumena, 0)                             as 'Δεσμευμένα Σχετικού Είδους',
       isnull(apothema.Apothema, 0) + isnull(anamenomena.Anamenomena, 0) - isnull(desmeumena.Desmeumena, 0) as 'ACTUAL RETAIL',
       (case when isnull(AnamonesPYP.Weighted_Average, 0) + isnull(AnamonesTQL.Weighted_Average, 0) + isnull(AnamonesDSV.Weighted_Average, 0) + isnull(AnamonesDC.Weighted_Average, 0) = 0 then PKOSTOS.PKOSTOS else isnull(AnamonesPYP.Weighted_Average, 0) + isnull(AnamonesTQL.Weighted_Average, 0) + isnull(AnamonesDSV.Weighted_Average, 0) + isnull(AnamonesDC.Weighted_Average, 0) end)  as 'πρότυπο κόστος',
       --isnull(AnamonesPYP.Weighted_Average, 0) + isnull(AnamonesTQL.Weighted_Average, 0) + isnull(AnamonesDSV.Weighted_Average, 0) + isnull(AnamonesDC.Weighted_Average, 0) as 'asd',
       isnull(FIFO.FIFO, FIFOLY.FIFO)                               as 'FIFO Price',
       (isnull((isnull(A.Punitsinstock, 0) + isnull(A.unitsinstock, 0) + isnull(A.DCunitsinstock, 0) + isnull(A.DSVunitsinstock, 0)) *  isnull(FIFO.FIFO, FIFOLY.FIFO), 0) 
                 +  isnull((isnull(AnamonesPYP.[Ποσ.1], 0) + isnull(AnamonesTQL.[Ποσ.1], 0) + isnull(AnamonesDSV.[Ποσ.1], 0) + isnull(AnamonesDC.[Ποσ.1], 0)) * (isnull(AnamonesPYP.Weighted_Average, 0) + isnull(AnamonesTQL.Weighted_Average, 0) + isnull(AnamonesDSV.Weighted_Average, 0) + isnull(AnamonesDC.Weighted_Average, 0)), 0)) 
                 / (isnull(A.Punitsinstock, 0) + isnull(A.unitsinstock, 0) + isnull(A.DCunitsinstock, 0) + isnull(A.DSVunitsinstock, 0) + isnull(AnamonesPYP.[Ποσ.1], 0) + isnull(AnamonesTQL.[Ποσ.1], 0) + isnull(AnamonesDSV.[Ποσ.1], 0) + isnull(AnamonesDC.[Ποσ.1], 0)) as 'Actual Costs',
       A.PRICER                                                     as 'Net Retail τιμή',
       isnull(ag.Promitheutes, '')                                  as 'Προμηθευτές'
FROM   ((select A.*, mtrcategoryEXP.NAME as CATNAME
                , mtrmanfctr.NAME   as MANNAME
                , TQL.unitsinstock
                --, ATQL.CustomerOrderUnits
                , DTQL.SupplierOrderUnits
                , Papada.unitsinstock 'Punitsinstock'
                , DPapada.CustomerOrderUnits 'PCustomerOrderUnits'
                , APapada.unitsinstock 'PSupplierOrderUnits'
                , DSV.unitsinstock 'DSVunitsinstock'
                --, ADSV.SupplierOrderUnits 'DSVSupplierOrderUnits'
                , DDSV.Desmeumena 'DSVCustomerOrderUnits'
                , StockDreamCargo.unitsinstock 'DCunitsinstock'
                --, AnamenomenaDreamCargo.SupplierOrderUnits 'DCSupplierOrderUnits'
                , DesmeumenaDreamCargo.CustomerOrderUnits 'DCCustomerOrderUnits'
            from mtrl A 
            join (select MTRCATEGORY, name from [dbo].[MTRCATEGORY] where name in ('Laptops & Smartphones', 'Laptops', 'Refurbished Laptops', 'Laptops', 'Outlet Laptops', 'Used Laptops') and company = 1000 AND isactive = 1) mtrcategoryEXP on mtrcategoryEXP.MTRCATEGORY = A.MTRCATEGORY
            left join (select * from [dbo].[MTRMANFCTR] where company = 1000 and isactive = 1) mtrmanfctr on mtrmanfctr.MTRMANFCTR = A.MTRMANFCTR
            left join (select sum(unitsinstock) 'unitsinstock', ProductID from [magicom_shop_2019].[dbo].[Tbl_Products_Suppliers] where supplierid IN (13, 19, 20, 39) group by ProductID) Papada on cast(Papada.ProductID  as varchar) = A.CODE
            left join (select sum(CustomerOrderUnits) 'CustomerOrderUnits', ProductID from [magicom_shop_2019].[dbo].[Tbl_Products_Suppliers] where supplierid IN (13, 19, 20, 39) group by ProductID) DPapada on cast(DPapada.ProductID  as varchar) = A.CODE
            left join (select unitsinstock, ProductID from [magicom_shop_2019].[dbo].[Tbl_Products_Suppliers] where supplierid = 18) APapada on cast(APapada.ProductID  as varchar) = A.CODE  COLLATE SQL_Latin1_General_CP1253_CI_AI
            left join (select unitsinstock, ProductID from [magicom_shop_2019].[dbo].[Tbl_Products_Suppliers] where supplierid = 41) TQL on cast(TQL.ProductID  as varchar) = A.CODE
            --left join (select CustomerOrderUnits, ProductID from [magicom_shop_2019].[dbo].[Tbl_Products_Suppliers] where supplierid = 41) ATQL on cast(ATQL.ProductID  as varchar) = A.CODE
            left join (select SupplierOrderUnits, ProductID from [magicom_shop_2019].[dbo].[Tbl_Products_Suppliers] where supplierid = 41) DTQL on cast(DTQL.ProductID  as varchar) = A.CODE
            left join (select unitsinstock, ProductID from [magicom_shop_2019].[dbo].[Tbl_Products_Suppliers] where supplierid = 43) DSV on cast(DSV.ProductID  as varchar) = A.CODE
            left join (select sum(CustomerOrderUnits) 'Desmeumena', ProductID from [magicom_shop_2019].[dbo].[Tbl_Products_Suppliers] where supplierid = 43 group by ProductID) DDSV on cast(DDSV.ProductID  as varchar) = A.CODE
            --left join (select unitsinstock 'SupplierOrderUnits', SupplierProductID from [magicom_shop_2019].[dbo].[Tbl_Products_Suppliers] where supplierid = 18) ADSV on cast(ADSV.SupplierProductID  as varchar) = 'ID' + A.CODE COLLATE SQL_Latin1_General_CP1253_CI_AI
            left join (select COALESCE(SUM(CASE WHEN supplierid IN (22) THEN unitsinstock ELSE 0 END), 0) 'UnitsInStock', ProductID from [magicom_shop_2019].[dbo].[Tbl_Products_Suppliers] GROUP BY productID) StockDreamCargo on cast(StockDreamCargo.ProductID  as varchar) = A.CODE
            --left join (select SupplierOrderUnits, ProductID from [magicom_shop_2019].[dbo].[Tbl_Products_Suppliers] where SupplierID = 22) AnamenomenaDreamCargo on cast(AnamenomenaDreamCargo.ProductID  as varchar) = A.CODE
            left join (select COALESCE(SUM(CASE WHEN supplierid IN (22) THEN CustomerOrderUnits ELSE 0 END), 0) 'CustomerOrderUnits', ProductID from [magicom_shop_2019].[dbo].[Tbl_Products_Suppliers] GROUP BY productID) DesmeumenaDreamCargo on cast(DesmeumenaDreamCargo.ProductID  as varchar) = A.CODE) A
       left join (select ag.mtrl
                            , STUFF((select Distinct ', ' + Prom from #agores ag2 where ag2.Description = ag.Description and "Prom" not in ('TOTAL QUALITY LOGISTICS ΑΕ', 'DREAM CARGO SERVICES SRL' , 'DSV AIR & SEA ΜΟΝΟΠΡΟΣΩΠΗ ΑΝΩΝΥΜΗ ΕΤΑΙΡΕΙΑ', 'DSV AIR &amp; SEA ΜΟΝΟΠΡΟΣΩΠΗ ΑΝΩΝΥΜΗ ΕΤΑΙΡΕΙΑ') FOR XML PATH ('')),1,2,'') AS 'Promitheutes'
                    from #agores ag
                    where "Prom" not in ('TOTAL QUALITY LOGISTICS ΑΕ', 'DREAM CARGO SERVICES SRL' , 'DSV AIR & SEA ΜΟΝΟΠΡΟΣΩΠΗ ΑΝΩΝΥΜΗ ΕΤΑΙΡΕΙΑ', 'DSV AIR &amp; SEA ΜΟΝΟΠΡΟΣΩΠΗ ΑΝΩΝΥΜΗ ΕΤΑΙΡΕΙΑ') 
                    group by Description, ag.mtrl, "Κωδικός Ε.", "Εμπορ.κατηγορία") ag on ag.mtrl = A.mtrl
       left join mtrdata B ON B.company = 1000 AND A.mtrl = B.mtrl AND B.fiscprd = 2025)
       left join mtrl D ON A.relitem = D.mtrl
       left join (select p.[Κωδικός Ε.], sum(p.[Ποσ.1]) as [Ποσ.1] from #pwlhseis p where datediff(day, "Ημερ/νία", GETDATE()) between 0 and 90 and FPRMS in (7201, 7205, 7203, 7207) group by p.[Κωδικός Ε.]) Actual90DaysSales on Actual90DaysSales.[Κωδικός Ε.] = A.CODE
       left join (select p.[Κωδικός Ε.], sum(p.[Ποσ.1]) as [Ποσ.1] from #pwlhseis p where datediff(day, "Ημερ/νία", GETDATE()) between 0 and 30 group by p.[Κωδικός Ε.]) Actual30DaysSales on Actual30DaysSales.[Κωδικός Ε.] = D.CODE
       
       left join (select p.[Κωδικός Ε.], sum(UNCOVQTY) as [Ποσ.1], sum("Τιμή Ευρώ"*UNCOVQTY)/sum(UNCOVQTY) as Weighted_Average from #agores p where "Α.Χ." in ('Κεντρικός Παπαδά', 'Κεντρικός Σολωμού', 'Κεντρικός Πειραιά', 'Κεντρικός Βουλιαγμένη') and "Τύπος" in ('Παραγγελία Σε Προμηθευτή', 'Packing list προμηθευτή') and UNCOVQTY <> 0 group by p.[Κωδικός Ε.]) AnamonesPYP on AnamonesPYP.[Κωδικός Ε.] = A.CODE
       left join (select p.[Κωδικός Ε.], sum(UNCOVQTY) as [Ποσ.1], sum("Τιμή Ευρώ"*UNCOVQTY)/sum(UNCOVQTY) as Weighted_Average from #agores p where "Α.Χ." = 'Total Quality Logistic' and "Τύπος" in ('Παραγγελία Σε Προμηθευτή', 'Packing list προμηθευτή') and UNCOVQTY <> 0 group by p.[Κωδικός Ε.]) AnamonesTQL on AnamonesTQL.[Κωδικός Ε.] = A.CODE
       left join (select p.[Κωδικός Ε.], sum(UNCOVQTY) as [Ποσ.1], sum("Τιμή Ευρώ"*UNCOVQTY)/sum(UNCOVQTY) as Weighted_Average from #agores p where "Α.Χ." = 'DSV' and "Τύπος" in ('Παραγγελία Σε Προμηθευτή', 'Packing list προμηθευτή') and UNCOVQTY <> 0 group by p.[Κωδικός Ε.]) AnamonesDSV on AnamonesDSV.[Κωδικός Ε.] = A.CODE
       left join (select p.[Κωδικός Ε.], sum(UNCOVQTY) as [Ποσ.1], sum("Τιμή Ευρώ"*UNCOVQTY)/sum(UNCOVQTY) as Weighted_Average from #agores p where "Α.Χ." = 'Dream Cargo' and "Τύπος" in ('Παραγγελία Σε Προμηθευτή', 'Packing list προμηθευτή') and UNCOVQTY <> 0 group by p.[Κωδικός Ε.]) AnamonesDC on AnamonesDC.[Κωδικός Ε.] = A.CODE
       
       --left join (select sum(unitsinstock) 'Apothema', SupplierProductID from [magicom_shop_2019].[dbo].[Tbl_Products_Suppliers] where SupplierID in (13, 19, 20, 39, 22, 41, 42, 43) group by SupplierProductID) apothema on apothema.SupplierProductID = D.CODE COLLATE SQL_Latin1_General_CP1253_CI_AI
       left join (select sum(unitsinstock) 'Apothema', SupplierProductID from [magicom_shop_2019].[dbo].[Tbl_Products_Suppliers] where SupplierID in (13, 19, 20, 39, 42) group by SupplierProductID) apothema on apothema.SupplierProductID = D.CODE COLLATE SQL_Latin1_General_CP1253_CI_AI
       left join (select unitsinstock 'Anamenomena', SupplierProductID from [magicom_shop_2019].[dbo].[Tbl_Products_Suppliers] where SupplierID = 18) anamenomena on anamenomena.SupplierProductID = 'ID'+D.CODE COLLATE SQL_Latin1_General_CP1253_CI_AI
       left join (select sum(CustomerOrderUnits) 'Desmeumena', SupplierProductID from [magicom_shop_2019].[dbo].[Tbl_Products_Suppliers] group by SupplierProductID) desmeumena on desmeumena.SupplierProductID = D.CODE COLLATE SQL_Latin1_General_CP1253_CI_AI
       left join (select sum(PURFIFO) 'FIFO', MTRL from MTRCPRICES where COMPANY=1000 AND FISCPRD=YEAR(GETDATE()) AND PERIOD=1000 group by MTRL) FIFO on FIFO.MTRL = A.MTRL
       left join (select sum(PURFIFO) 'FIFO', MTRL from MTRCPRICES where COMPANY=1000 AND FISCPRD=YEAR(GETDATE())-1 AND PERIOD=1000 group by MTRL) FIFOLY on FIFOLY.MTRL = A.MTRL
       left join (select sum(STANDCOST) 'PKOSTOS', MTRL from MTRCPRICES where COMPANY=1000 AND FISCPRD=YEAR(GETDATE()) AND PERIOD=1000 group by MTRL) PKOSTOS on PKOSTOS.MTRL = A.MTRL
       left join (select sum(STANDCOST) 'PKOSTOS', MTRL from MTRCPRICES where COMPANY=1000 AND FISCPRD=YEAR(GETDATE())-1 AND PERIOD=1000 group by MTRL) PKOSTOSLY on PKOSTOSLY.MTRL = A.MTRL
WHERE  A.company = 1000
       and A.sodtype = 51
       and ((isnull(A.Punitsinstock, 0) + isnull(A.unitsinstock, 0) + isnull(A.DCunitsinstock, 0) + isnull(A.DSVunitsinstock, 0) + ISNULL(A.PSupplierOrderUnits, 0)) - (isnull(A.PCustomerOrderUnits, 0) + isnull(A.SupplierOrderUnits, 0) + isnull(A.DCCustomerOrderUnits, 0) + isnull(A.DSVCustomerOrderUnits, 0)) > 9 or isnull(A.PSupplierOrderUnits, 0) > 9
                or (case when A.CATNAME like '%efurbi%' then isnull(A.Punitsinstock, 0) + isnull(A.unitsinstock, 0) + isnull(A.DCunitsinstock, 0) + isnull(A.DSVunitsinstock, 0) + isnull(AnamonesPYP.[Ποσ.1], 0) + isnull(AnamonesTQL.[Ποσ.1], 0) + isnull(AnamonesDSV.[Ποσ.1], 0) + isnull(AnamonesDC.[Ποσ.1], 0) else 0 end) > 5)
       and isnull(A.MANNAME, '') <> 'Apple'

-- Ensure previous #detailed temp is removed before creating a fresh one.
-- #detailed: per-item detailed report derived from #main_table used for
-- manual price/special adjustments and to present final retail/export mapping.
drop table if exists #detailed
select a.code                                                           as 'MG ID',
       "Κατηγορία"                                                      as 'Κατηγορία',
       "Κατασκευαστής"                                                  as 'Κατασκευαστής',
       a.NAME                                                           as 'Description',
       --"Ready Stock"                                                  as 'Ready Stock',
       --"Des"                                                          as 'Des',
       convert(int, "Ready Stock-Δεσμευμένα")                            as 'Ready Stock-Δεσμευμένα',
       convert(int, "Incoming stock")                                    as 'Incoming stock',
       convert(int, convert(float, "Actual Stock"))                                                   as 'Actual Stock Retail only', --convert(decimal(10,2), "Actual Stock "),
       convert(int, "Actual Stock B2B")                                           as 'Actual Stock B2B',
       "Εικονικά Αναμενόμενα"                                           as 'Εικονικά Αναμενόμενα',
       convert(int, convert(float, case when "Actual Stock" <> '' then "Actual Stock" else "Final Stock" end)) as 'Final Stock',
       "Actual 90 Days Sales"                                           as 'Actual 90 Days Sales (EXP)',
       "Actual 30 Days Sales"                                           as 'Actual 30 Days Sales (Retail ID)',
       convert(decimal(10,2), "πρότυπο κόστος")                         as 'πρότυπο κόστος',
       convert(decimal(10,2), "FIFO Price")                             as 'FIFO Price',
       convert(decimal(10,2), "Net Retail τιμή")                        as 'Net Retail τιμή',
       convert(decimal(10,2), "Actual Costs")                           as 'Actual Costs',
       a."Part No Export"                                               as 'Part No Export',
       "Specs"                                                          as 'Specs',
       a."Κωδικός"                                                      as 'Κωδικός (Συσχέτισης)',
       convert(int, "Απόθεμα Σχετικού Είδους")                           as 'Απόθεμα Σχετικού Είδους',
       convert(int, "Αναμενόμενα Σχετικού Είδους")                       as 'Αναμενόμενα Σχετικού Είδους',
       convert(int, "Δεσμευμένα Σχετικού Είδους")                        as 'Δεσμευμένα Σχετικού Είδους',
       convert(int, "ACTUAL RETAIL")                                     as 'ACTUAL RETAIL',
       ''                                                               as 'Last Special B2B Price €',
       ''                                                               as 'New Special B2B Price €',
       ''                                                               as 'STATUS',
       "Προμηθευτές"                                                    as 'Προμηθευτής 1'
into #detailed
from (select * from #main_table where ("Actual Stock B2B" > 0 or "Actual Stock B2B" is null)) a
where code not in (select "Κωδικός" FROM #main_table where NAME like '%EXP%')

-- Remove any existing #headers temp; we'll build a header/template row per category
-- #headers acts as a template (NULL columns) so final output can be grouped
-- and display category header rows before the detail rows.
drop table if exists #headers
SELECT distinct
        CAST(NULL AS nvarchar(200)) AS [MG ID],
        (case when c.[Κατηγορία] like '%efurbished%' then 'Refurbirshed' else c.[Κατηγορία] end)  AS [Κατηγορία],
        CAST(NULL AS nvarchar(200)) AS [Κατασκευαστής],
        CAST(NULL AS nvarchar(200)) AS [Description],
        --CAST(NULL AS decimal(10,2)) AS [Ready Stock],
        --CAST(NULL AS decimal(10,2)) AS [Des],
        CAST(NULL AS int) AS [Ready Stock-Δεσμευμένα],
        CAST(NULL AS int) AS [Incoming stock],
        CAST(NULL AS int) AS [Actual Stock Retail only],
        CAST(NULL AS int) AS [Actual Stock B2B],
        CAST(NULL AS int) AS [Εικονικά Αναμενόμενα],
        CAST(NULL AS nvarchar(200)) AS [Final Stock],
        CAST(NULL AS nvarchar(200)) AS [Actual 90 Days Sales (EXP)],
        CAST(NULL AS nvarchar(200)) AS [Actual 30 Days Sales (Retail ID)],
        CAST(NULL AS decimal(10,2)) AS [πρότυπο κόστος],
        CAST(NULL AS decimal(10,2)) AS [FIFO Price],
        CAST(NULL AS decimal(10,2)) AS [Net Retail τιμή],
        CAST(NULL AS decimal(10,2)) AS [Actual Costs],
        CAST(NULL AS nvarchar(200)) AS [Part No Export],
        CAST(NULL AS nvarchar(200)) AS [Specs],
        CAST(NULL AS nvarchar(200)) AS [Κωδικός (Συσχέτισης)],
        CAST(NULL AS int) AS [Απόθεμα Σχετικού Είδους],
        CAST(NULL AS int) AS [Αναμενόμενα Σχετικού Είδους],
        CAST(NULL AS int) AS [Δεσμευμένα Σχετικού Είδους],
        CAST(NULL AS nvarchar(200)) AS [ACTUAL RETAIL],
        CAST(NULL AS nvarchar(50))  AS [Last Special B2B Price €],
        CAST(NULL AS nvarchar(50))  AS [New Special B2B Price €],
        CAST(NULL AS nvarchar(50))  AS [STATUS],
              CAST(NULL AS nvarchar(200)) AS [Προμηθευτής 1]
       -- Insert a NULL-filled template row per distinct category into #headers.
       -- This creates a header/template row for each category so the final
       -- report can interleave category headers with detail rows.
       into #headers
       FROM (SELECT DISTINCT [Κατηγορία] FROM #main_table) AS c

drop table if exists #f
SELECT "MG ID",
       _hdr,
       "Κατηγορία",
       "Κατασκευαστής",
       "Description",
       "Ready Stock-Δεσμευμένα",
       "Incoming stock",
       "Actual Stock Retail only",
       "Actual Stock B2B",
       "Εικονικά Αναμενόμενα",
       "Final Stock",
       "Actual 90 Days Sales (EXP)",
       "Actual 30 Days Sales (Retail ID)",
       "πρότυπο κόστος",
       "FIFO Price",
       "Net Retail τιμή",
       "Actual Costs",
       "Part No Export",
       "Specs",
       "Κωδικός (Συσχέτισης)",
       "Απόθεμα Σχετικού Είδους",
       "Αναμενόμενα Σχετικού Είδους",
       "Δεσμευμένα Σχετικού Είδους",
       "ACTUAL RETAIL",
       "Last Special B2B Price €",
       "New Special B2B Price €",
       "STATUS",
       "Προμηθευτής 1"
into #f
FROM (
    SELECT 0 AS _hdr, h.* FROM #headers AS h where "Κατηγορία" in (select "Κατηγορία" from #detailed) or "Κατηγορία" = 'Refurbirshed'
    UNION ALL
    SELECT 1 AS _hdr, d.* FROM #detailed AS d
) x

select "MG ID",
       "Κατηγορία",
       "Κατασκευαστής",
       "Description",
       "Ready Stock-Δεσμευμένα",
       "Incoming stock",
       "Actual Stock Retail only",
       "Actual Stock B2B",
       "Εικονικά Αναμενόμενα",
       "Final Stock",
       "Actual 90 Days Sales (EXP)",
       "Actual 30 Days Sales (Retail ID)",
       "πρότυπο κόστος",
       "FIFO Price",
       "Net Retail τιμή",
       "Actual Costs",
       "Part No Export",
       "Specs",
       "Κωδικός (Συσχέτισης)",
       "Απόθεμα Σχετικού Είδους",
       "Αναμενόμενα Σχετικού Είδους",
       "Δεσμευμένα Σχετικού Είδους",
       "ACTUAL RETAIL",
       "Last Special B2B Price €",
       "New Special B2B Price €",
       "STATUS",
       "Προμηθευτής 1"
       from (
select *
from (
select * 
from #f
where "Κατηγορία" in ('Exp Laptops', 'Laptops', 'Exp Tablets', 'EXP Gaming Consoles', 'Exp PCs', 'Exp Monitors', 'EXP Air Fryers', 'EXP Blenders', 'Exp Keyboards', 'Exp Docking Stations', 'Exp Network')
order by case when "Κατηγορία" =    'Exp Laptops'           then 1
                when "Κατηγορία" =  'Laptops'               then 2
                when "Κατηγορία" =  'Exp Tablets'           then 3
                when "Κατηγορία" =  'EXP Gaming Consoles'   then 4
                when "Κατηγορία" =  'Exp PCs'               then 5
                when "Κατηγορία" =  'Exp Monitors'          then 6
                when "Κατηγορία" =  'EXP Air Fryers'        then 7
                when "Κατηγορία" =  'EXP Blenders'          then 8
                when "Κατηγορία" =  'Exp Keyboards'         then 9
                when "Κατηγορία" =  'Exp Docking Stations'  then 10
                when "Κατηγορία" =  'Exp Network'           then 11
                end,
                _hdr,
                "Final Stock" desc,
                [Description] OFFSET 0 ROWS) a
union all
select *
from (
select * 
from #f
where "Κατηγορία" not in ('Exp Laptops', 'Laptops', 'Exp Tablets', 'EXP Gaming Consoles', 'Exp PCs', 'Exp Monitors', 'EXP Air Fryers', 'EXP Blenders', 'Exp Keyboards', 'Exp Docking Stations', 'Exp Network')
        and "Κατηγορία" not like  '%efurbish%' and "Κατηγορία" not like  '%efubish%' --'Refurbirshed'
order by "Κατηγορία",
                _hdr,
                "Final Stock" desc,
                [Description] OFFSET 0 ROWS) b 
union all
select *
from (
select * 
from #f
where Κατηγορία like '%efurbish%' or "Κατηγορία" like  '%efubish%'
order by "Κατηγορία",
                _hdr,
                "Final Stock" desc,
                [Description] OFFSET 0 ROWS) c) a

drop table if exists #agores
drop table if exists #pwlhseis
drop table if exists #main_table
drop table if exists #detailed
drop table if exists #headers

END
