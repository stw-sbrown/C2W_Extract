# C2W_Extract: 
 Competing To Win: Market Operator Extract 2016.
 
The Subversion structure was 
Trunk (latest Revision 6497), later Tags suffixed MOSL, current Tag MSOL_Build106
Branches/SAP (branched at Trunk Revision: 4028, current Revision 6499 / tag SAP_Build76)
Branches/FIN (branched at Trunk Revision: 4047, Last Tag FIN_Build01 REV 5391, current Revision 5458 ) 

The different branches were different versions of the extract for different systems.
They are all required, although no further activity is expected.

##SUBVERISON Migration
This is basically a tip migration of the Subversion Repository https://stwl125.stw.intra/svn/target/C2W_MO_Extract at REVISION 6500
with branches.

You might think it possible to reintroduce the merge history from trunk to SAP & FIN, but the branches were not consitantly merging from Trunk (some manual).

For this reason the migration will recreate the Git Repo Branches over several versions of the repo.
The origin of each Branch will be imported and then brought up to the tip.
No merging in Git.

1. Import SVN Trunk REV 4028 (SAP Branch point) as GIT Main 
2. Tag Git Main as main_svn_trunk_rev4028

3. Create Git Branch SAP from current Git Main
4. Import SVN Branch SAP Tip REV 6499 to Git SAP
5. Create Git Tag SAP_Build76

6. Import SVN Trunk REV 4047 (FIN branch point)
7. Tag Git Main as main_svn_trunk_rev4047
8. Create Git Branch FIN from current Git Main (done from GitHub)

9. Import SVN Branch FIN REV 5391 (FIN_Build01)
10. Create Git Tag FIN_Build01

11. Import SVN Branch FIN REV 5458 (tip)
12. Checked against SVN and removed files and folders from updates (imports are just snapshots)
12. Create Git Tag FIN_SVN_TIP
--here


13. Import SVN Trunk Tip REV6407 (tip)
14. Tag Git Main as main_MSOL_Build106
