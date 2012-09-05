
declare @atclevelid int, @mplevelid int, @inglevelid int

set @atclevelid=10
set @mplevelid=11
set @inglevelid=12


/*
/* Get all term/code node data*/
set nocount on
select Term, Code from
(
	select 'Term' as Term, 'Code' as Code, 1 as x
	union all
	select term.Term_ENG as Term, Code,2 as x
	from medicaldictionaryterm term
	where term.DictionaryLevelId in (@atclevelid, @mplevelid, @inglevelid) and 1 between term.FromVersionOrdinal and term.ToVersionOrdinal
)
as terms
*/


/* get all child/parent relationships */
/* get top level, then other atcs, then mp and ing */
/* highest-level nodes will relate to parent 'TopOfDictionary' */
set nocount on
select ChildCode, ParentCode, Level from
(
	select 'ChildCode' as ChildCode, 'ParentCode' as ParentCode, 'Level' as Level, 1 as x
	union all
	select term.Code as ChildCode, 'Top' as ParentCode, 'ATC1' as Level, 2 as x
	from MedicalDictionaryTerm term
	where term.DictionaryLevelId=@atclevelid and 1 between term.FromVersionOrdinal and term.ToVersionOrdinal
	and term.LevelRecursiveDepth=1 -- ATC1 level.

	union all

	select child.Code as ChildCode, term.Code as ParentCode,
		case when term.LevelRecursiveDepth =1 then 'ATC2' 
			when term.LevelRecursiveDepth=2 then 'ATC3'
			when term.LevelRecursiveDepth=3 then 'ATC4'
		end as Level
		, 2 as x
	from MedicalDictionaryTerm term
	join MedicalDictionaryTermRel rel on term.TermId=rel.FromTermId
	join MedicalDictionaryTerm child on child.TermId=rel.ToTermId
	where term.DictionaryLevelId=@atclevelid 
	and 1 between term.FromVersionOrdinal and term.ToVersionOrdinal
	and 1 between child.FromVersionOrdinal and child.ToVersionOrdinal
	and term.LevelRecursiveDepth <> 4 -- filter out children.

	union all

	select child.Code as ChildCode, term.Code as ParentCode, 
	case when term.DictionaryLevelId=@mplevelid then 'MP' else 'ING' end as Level
	, 2 as x
	from MedicalDictionaryTerm term
	join MedicalDictionaryTermRel rel on term.TermId=rel.FromTermId
	join MedicalDictionaryTerm child on child.TermId=rel.ToTermId
	where term.DictionaryLevelId in (@mplevelid, @inglevelid) 
	and 1 between term.FromVersionOrdinal and term.ToVersionOrdinal
	and 1 between child.FromVersionOrdinal and child.ToVersionOrdinal
) as termrels



/* Get component english text node data*/
set nocount on
select ComponentName from
(
	select 'ComponentName' as ComponentName, 1 as x  
	union all 
	select distinct --top 10
	C_ENG.Name as Value, 2 as x 
	from MedicalDictTermComponents mdtc 
	inner join MedicalDictionaryLevel l on l.MedicalDictionaryID=mdtc.MedicalDictionaryID
	inner join ComponentEngStrings C_ENG ON C_ENG.Id = mdtc.ENGStringID
	where 1 between mdtc.FromVersionOrdinal and mdtc.ToVersionOrdinal
	and (l.DictionaryLevelId=@mplevelid or l.DictionaryLevelId=@inglevelid)
) as componentoutput
where rtrim(componentoutput.ComponentName) <> ''


/* Get all term/component relationships */
-- 29205804 rows returned, took 42 minutes.
set nocount on
select Component, ComponentType, Code, Level from
(
	select 'Component' as Component, 'ComponentType' as ComponentType, 'Code' as Code, 'Level' as Level 
	union all
	select distinct ces.Name as Component, ctypes.OID as ComponentType, t.Code as Code, 
		case when t.DictionaryLevelId=@mplevelid then 'MP' else 'ING' end as Level
	from MedicalDictTermComponents mdtc 
	join medicaldictionaryterm t on mdtc.TermID=t.TermId
	join MedicalDictComponentTypes ctypes on ctypes.ComponentTypeID=mdtc.ComponentTypeID
	join ComponentEngStrings ces on ces.Id=mdtc.ENGStringID
	where 1 between mdtc.FromVersionOrdinal and mdtc.ToVersionOrdinal
	and (t.DictionaryLevelId=@mplevelid or t.DictionaryLevelId=@inglevelid)
) as results
where rtrim(Component)<>''




/*
testing...

select * from MedicalDictionary
-- 3=whodrugc, 1=meddra

select * from MedicalDictionaryLevel where MedicalDictionaryID=3

select * from MedicalDictionaryTerm where DictionaryLevelId=12

select level.OID, term.* from medicaldictionaryterm term
inner join MedicalDictionaryLevel level on term.DictionaryLevelId=level.DictionaryLevelId
where term.TermId in (1493290, 397595, 396614, 396612, 396611, 396586)


select * from MedicalDictLevelComponents where DictionaryLevelId=11 and FromVersionOrdinal=1 and ToVersionOrdinal=2
-- level 11=MP, 12=ING
-- in whodrugc, 23 components for mp, 14 for ing

select * from MedicalDictComponentTypes where MedicalDictionaryID=3

select * from MedicalDictTermComponents
where TermId in (1493290, 397595, 396614, 396612, 396611, 396586)  

select * from MedicalDictLevelComponents where DictionaryLevelId in (10,11,12)

select * from ComponentEngStrings

-- get counts of terms for nodes.  

select COUNT(*) from medicaldictionaryterm term
where term.DictionaryLevelId=12 and term.FromVersionOrdinal=1 and term.ToVersionOrdinal=2

-- whodrugC node counts by level:
--10/atc=1216
--11/mp=1,072,870
--12/ingredient=1,447,439

----------------- COMPONENTS ------------------

select COUNT(*) from MedicalDictLevelComponents where DictionaryLevelId=11 and FromVersionOrdinal=1 and ToVersionOrdinal=2
-- in whodrugc, 23 components for mp, 14 for ing

select COUNT(*) from MedicalDictTermComponents mdtc 
inner join medicaldictionaryterm t on mdtc.TermID=t.TermId
where mdtc.FromVersionOrdinal=1 and mdtc.ToVersionOrdinal=2
and t.DictionaryLevelId=12

select top 100 mdtc.* from MedicalDictTermComponents mdtc 
inner join medicaldictionaryterm t on mdtc.TermID=t.TermId

where mdtc.FromVersionOrdinal=1 and mdtc.ToVersionOrdinal=2
and t.DictionaryLevelId=12

-- whodrugc: mp count=24,597,584
-- ing count=18,845,172
*/

-----------------------------------------------------------------------------------------------
-- code-level pairing is a unique identifier.  

/*
--get full list of ATC level 1 terms for version #1
set nocount on
;with cte (l,Term,Code,x)
as 
(
select 'Level' as l, 'Term' as Term, 'Code' as Code, 2
union 
select case when term.LevelRecursiveDepth= 1 then 'ATC1'
	when term.LevelRecursiveDepth = 2 then 'ATC2'
	when term.LevelRecursiveDepth = 3 then 'ATC3'
	when term.LevelRecursiveDepth = 4 then 'ATC4'
	end as l
 ,term.Term_ENG as Term, Code,3
 from medicaldictionaryterm term
where term.DictionaryLevelId=10 and term.FromVersionOrdinal=1 and term.ToVersionOrdinal=2
)
select l, Term, Code from cte

-- MPs 1,072,870
set nocount on
;with cteMP (l,Term,Code,x)
as 
(
select 'Level' as l, 'Term' as Term, 'Code' as Code, 1
union 
select top 1 'MP' as Level, term.Term_ENG as Term, Code, 2 from medicaldictionaryterm term
where term.DictionaryLevelId=11 and term.FromVersionOrdinal=1 and term.ToVersionOrdinal=2 
)
select l, Term, Code from cteM

-- INGs 1,447,439
set nocount on
;with cteING (l,Term,Code,x)
as 
(
select 'Level' as l, 'Term' as Term, 'Code' as Code, 1
union 
select 'ING' as Level, Term_ENG as Term, Code, 2 from medicaldictionaryterm term
where term.DictionaryLevelId=12 and term.FromVersionOrdinal=1 and term.ToVersionOrdinal=2
)
select l, Term, Code from cteING
*/