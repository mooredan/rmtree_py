-- MasterSources.sql
-- ve3meo 2011-11-04
-- Lists Master Sources and fields from SourceTable with 
-- name/id of Source Template and number of citations
-- 2011-11-06 now reports Free Form and orphaned citations; count of media items linked to master source; improved format of SrcFields

-- BEGIN
SELECT   SourceTable.SourceID AS 'SrcID',
         SourceTable.Name COLLATE NOCASE AS 'Source Name',
         SourceTable.RefNumber,
-- SourceTable FIELDS parsed out from XML in a blob
         REPLACE(                                                                                                
          REPLACE(
           REPLACE(
            REPLACE(
             REPLACE(
              REPLACE(SUBSTR(CAST(SourceTable.FIELDS AS TEXT),55, LENGTH(CAST(SourceTable.FIELDS AS TEXT))-87), '</Name>', ':'||CAST (X'09' AS TEXT)),
             '<Field><Name>', ''),
            '</Value>',''),
           '<Value>',''),
          '<Value/>',''),
         '</Field>',CAST (X'0D' AS TEXT))
         AS 'Src Fields',
--
         SourceTable.ActualText,
         SourceTable.Comments,
         MediaCtr,
         SourceTable.IsPrivate,
         COUNT(1) AS Citations,
         CASE
          WHEN SourceTable.TemplateID=0 THEN 'Free Form'
          WHEN SourceTemplateTable.TemplateID THEN SourceTemplateTable.Name COLLATE NOCASE
          ELSE 'ERROR: source template does not exist'
         END AS 'Template',
         SourceTable.TemplateID AS 'TpltID'
FROM     CitationTable 
         LEFT JOIN SourceTable USING (SourceID)
         LEFT JOIN SourceTemplateTable USING (TemplateID)
         LEFT JOIN
-- count media items linkd to master source
(SELECT MediaLinkTable.OwnerID AS SourceID, COUNT() AS MediaCtr
FROM multimediatable
LEFT JOIN MediaLinkTable USING(MediaID)
WHERE MediaLinkTable.OwnerType=3
GROUP BY SourceID 
)
USING (SourceID)
GROUP BY SourceTable.SourceID
ORDER BY "Source Name"
; -- END
