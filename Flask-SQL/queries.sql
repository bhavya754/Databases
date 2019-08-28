-- custom query 1
select ap.playerID, ap.teamID, ap.yearID, ap.G_all, ba.AB, ba.H as hits, f.A as assists, f.E as errors from Appearances ap inner join batting ba on ap.playerID = ba.playerid and ap.teamId = ba.teamId and ap.yearId = ba.yearId inner join fielding f on ap.playerid = f.playerid and ap.teamId = f.teamId and ap.yearId = f.yearId where ap.playerid=playerID ;   

-- custom query 2
select a1.playerid as player, a2.playerid as teammate, min(a2.yearId) as firstYear, max(a2.yearid) as lastYear, count(*) as total_appearances from Appearances a1 inner join Appearances a2 on a1.teamId=a2.teamId and a1.playerid <> a2.playerid and a1.yearId=a2.yearId where a1.playerid = playerID group by a1.playerid, a2.playerid order by a2.playerid;

-- custom query 2
select p.nameLast, p.nameFirst, ap.playerID, ap.teamID, ap.yearID, ap.G_all, ba.AB, ba.H as hits, sum(f.A) as assists, sum(f.E) as errors1 from Appearances ap inner join batting ba on ap.playerID = ba.playerid and ap.teamId = ba.teamId and ap.yearId = ba.yearId Inner join fielding f on ap.playerid = f.playerid and ap.teamId = f.teamId and ap.yearId = f.yearId inner join people p on ap.playerid=p.playerID where ap.teamId=teamId+ and ap.yearId=yearid group by p.nameLast, p.nameFirst, ap.playerID,ap.teamID, ap.yearID, ap.G_all, ba.AB, hits order by p.playerid