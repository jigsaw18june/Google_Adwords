create table Queries
(
qid integer NOT NULL,
query varchar(400),
primary key (qid)
);

create table Advertisers 
(
advertiserId integer,
budget float,
ctc float,
balance1 float,
ctcDisplayCount1 int,
balance2 float,
ctcDisplayCount2 int,
balance3 float,
ctcDisplayCount3 int,
balance4 float,
ctcDisplayCount4 int,
balance5 float,
ctcDisplayCount5 int,
balance6 float,
ctcDisplayCount6 int,
primary key (advertiserId)
);

create table Keywords
(
advertiserId integer,
keyword varchar(100),
bid float,
primary key (advertiserId, keyword),
foreign key (advertiserId) references Advertisers 
);


create table Tokens 
(
qid integer,
token varchar(200)
);

CREATE TABLE FullTable
(AdvertiserId int,
 qId int,
 MatchCount int,
 KeywordCount int,
 tokCount int,
 CTC float,
 totalBid float,
 budget float,
 balance1 float,
 ctcDisplayCount1 int,
 balance2 float,
 ctcDisplayCount2 int,
 balance3 float,
 ctcDisplayCount3 int,
 balance4 float,
 ctcDisplayCount4 int,
 balance5 float,
 ctcDisplayCount5 int,
 balance6 float,
 ctcDisplayCount6 int,
 Similarity float,
 QualityScore float,
 adRank1 float,
 adRank2 float,
 adRank3 float,
 adRank4 float,
 adRank5 float,
 adRank6 float
 );
 
Create Table OutputList1
(
qId int,
rank int,
AdvertiserId int,
balance float,
budget float
);

Create Table OutputList2
(
qId int,
rank int,
AdvertiserId int,
balance float,
budget float
);

Create Table OutputList3
(
qId int,
rank int,
AdvertiserId int,
balance float,
budget float
);

Create Table OutputList4
(
qId int,
rank int,
AdvertiserId int,
balance float,
budget float
);

Create Table OutputList5
(
qId int,
rank int,
AdvertiserId int,
balance float,
budget float
);

Create Table OutputList6
(
qId int,
rank int,
AdvertiserId int,
balance float,
budget float
);

CREATE TABLE CombinedAQK(AdvertiserId int , qId int , MatchCount int , totalBid float , CTC float , budget float);

CREATE TABLE QueryTokenCount(qId int , tokenCount int);

CREATE OR REPLACE PROCEDURE tokenizesingle(queryId IN int)
IS
  v_token Queries.query%TYPE;
  v_space_position NUMBER := 1;
  v_prev_space_position NUMBER := 1;
BEGIN
  DELETE FROM TOKENS;
  FOR v_rec IN (SELECT qid, query FROM Queries WHERE qid = queryId)
  LOOP
    v_prev_space_position := 0;

    LOOP
      v_space_position := instr(v_rec.query, ' ', v_prev_space_position + 1);
      IF v_space_position > 0 THEN
        v_token := substr(v_rec.query, v_prev_space_position + 1, v_space_position - v_prev_space_position - 1);
      ELSE
        v_token := substr(v_rec.query, v_prev_space_position + 1);
      END IF;

      INSERT INTO Tokens VALUES (v_rec.qid, v_token);

      v_prev_space_position := v_space_position;
      EXIT WHEN v_space_position = 0;
    END LOOP;
  END LOOP;

  COMMIT;
END;
/


CREATE OR REPLACE PROCEDURE adwords
IS

begin
DELETE FROM CombinedAQK;
INSERT INTO CombinedAQK
SELECT A.AdvertiserId, T.qId, count(*), null, A.CTC, A.budget
FROM Tokens T, Advertisers A, Keywords K
WHERE A.AdvertiserId = K.AdvertiserId AND K.keyword = T.token
GROUP BY A.AdvertiserId, T.qId, A.CTC, A.budget;

UPDATE CombinedAQK Com SET totalBid =  (select sum( Key.bid) from keywords Key,
Advertisers Adv, (SELECT DISTINCT TOKEN FROM Tokens) T
WHERE Key.keyword = T.token AND Adv.AdvertiserId = Key.AdvertiserId AND  Com.advertiserId = Key.AdvertiserId
GROUP BY Adv.AdvertiserId);

DELETE FROM QueryTokenCount;
INSERT INTO QueryTokenCount
SELECT P.qId, sum(P.Powers)
FROM (SELECT T.qId, (power(count(*),2)) AS Powers
 FROM Tokens T
 GROUP BY qId, token) P
GROUP BY qId;

DELETE FROM FullTable;
INSERT INTO FullTable
SELECT C.AdvertiserId, C.qId, C.MatchCount, K.KeywordCount, Q.tokenCount, C.CTC, C.totalBid, C.budget, 
A.balance1, A.ctcDisplayCount1, A.balance2, A.ctcDisplayCount2, A.balance3, A.ctcDisplayCount3,
A.balance4, A.ctcDisplayCount4, A.balance5, A.ctcDisplayCount5, A.balance6, A.ctcDisplayCount6, null, null, 
null, null, null, null, null, null
FROM Advertisers A,CombinedAQK C, AdvKeyCount K, QueryTokenCount Q
WHERE A.AdvertiserId = C.AdvertiserId AND C.AdvertiserId = K.AdvertiserId;


UPDATE FullTable SET Similarity=MatchCount/(SQRT(tokCount*KeywordCount));
UPDATE FullTable SET QualityScore = CTC*Similarity;
UPDATE FullTable SET adrank1 = QualityScore*totalbid;


UPDATE FullTable SET adrank2 = QualityScore*totalbid;


UPDATE FullTable SET adrank3 = QualityScore*Balance3;


UPDATE FullTable SET adrank4 = QualityScore*Balance4;


UPDATE FullTable SET adrank5 = QualityScore*totalbid*(1-EXP(-Balance5/Budget));


UPDATE FullTable SET adrank6 = QualityScore*totalbid*(1-EXP(-Balance6/Budget));


end;
/



CREATE OR REPLACE PROCEDURE display (queryId IN int, k1 IN int, k2 IN int, k3 IN int, k4 IN int, k5 IN int, k6 IN int)
IS
	--queryId NUMBER := 1;
	rank NUMBER := 1;
	advId NUMBER := 0;
	nextBid	 float;
	BEGIN
		--LOOP
			FOR record IN (SELECT qid, AdvertiserId, budget, balance1, CTC, totalBid, ctcDisplayCount1, adRank1 FROM
							(SELECT qid, AdvertiserId, budget, balance1, CTC, totalBid, ctcDisplayCount1, adRank1 FROM FullTable
							 WHERE balance1 >= totalBid
							 ORDER BY adRank1 Desc, AdvertiserId Asc) WHERE ROWNUM <= k1)

			
			LOOP
				--SELECT ctcDisplayCount1 into advId from Advertisers WHERE Advertisers.AdvertiserId = record.AdvertiserId;
				IF (record.CTC*100 > record.ctcDisplayCount1) THEN
					UPDATE Advertisers
					SET balance1 = balance1 - record.totalBid
					WHERE Advertisers.AdvertiserId = record.Advertiserid; 
					INSERT INTO OutputList1 VALUES(record.qid, rank, record.AdvertiserId, record.balance1 - record.totalBid, record.budget);
				ELSE
					INSERT INTO OutputList1 VALUES(record.qid, rank, record.AdvertiserId, record.balance1, record.budget);
				
				END IF;

				IF (record.ctcDisplayCount1 < 99) THEN
					UPDATE Advertisers
					SET ctcDisplayCount1 = ctcDisplayCount1 + 1
					WHERE Advertisers.AdvertiserId = record.Advertiserid;
				ELSE
					UPDATE Advertisers
					SET ctcDisplayCount1 = 0
					WHERE Advertisers.AdvertiserId = record.Advertiserid;
				END IF;
				rank := rank + 1;
			END LOOP;
			--DELETE FROM FullTable;
			--queryId := queryId + 1;
			--rank := 1;
			--EXIT WHEN queryId = queryId + 1;
		--END LOOP;
		
		rank := 1;
		advId := 0;
		
		FOR record IN (SELECT qid, AdvertiserId, budget, balance3, CTC, totalBid, ctcDisplayCount3, adRank3 FROM
							(SELECT qid, AdvertiserId, budget, balance3, CTC, totalBid, ctcDisplayCount3, adRank3 FROM FullTable
							 WHERE balance3 >= totalBid
							 ORDER BY adRank3 Desc, AdvertiserId Asc) WHERE ROWNUM <= k3)

			
			LOOP
				--SELECT ctcDisplayCount3 into advId from Advertisers WHERE Advertisers.AdvertiserId = record.AdvertiserId;
				IF (record.CTC*100 > record.ctcDisplayCount3) THEN
					UPDATE Advertisers
					SET balance3 = balance3 - record.totalBid
					WHERE Advertisers.AdvertiserId = record.Advertiserid;

					INSERT INTO OutputList3 VALUES(record.qid, rank, record.AdvertiserId, record.balance3 - record.totalBid, record.budget);
				ELSE
					INSERT INTO OutputList3 VALUES(record.qid, rank, record.AdvertiserId, record.balance3, record.budget);
				END IF;

				IF (record.ctcDisplayCount3 < 99) THEN
					UPDATE Advertisers
					SET ctcDisplayCount3 = ctcDisplayCount3 + 1
					WHERE Advertisers.AdvertiserId = record.Advertiserid;
				ELSE
					UPDATE Advertisers
					SET ctcDisplayCount3 = 0
					WHERE Advertisers.AdvertiserId = record.Advertiserid;
				END IF;
				rank := rank + 1;
			END LOOP;
			--DELETE FROM FullTable;
			rank := 1;
			advId := 0;
			
			FOR record IN (SELECT qid, AdvertiserId, budget, balance5, CTC, totalBid, ctcDisplayCount5, adRank5 FROM
							(SELECT qid, AdvertiserId, budget, balance5, CTC, totalBid, ctcDisplayCount5, adRank5 FROM FullTable
							 WHERE balance5 >= totalBid
							 ORDER BY adRank5 Desc, AdvertiserId Asc) WHERE ROWNUM <= k5)

			
			LOOP
				--SELECT ctcDisplayCount5 into advId from Advertisers WHERE Advertisers.AdvertiserId = record.AdvertiserId;

				IF (record.CTC*100 > record.ctcDisplayCount5) THEN
					UPDATE Advertisers
					SET balance5 = balance5 - record.totalBid
					WHERE Advertisers.AdvertiserId = record.Advertiserid;

					INSERT INTO OutputList5 VALUES(record.qid, rank, record.AdvertiserId, record.balance5 - record.totalBid, record.budget);
				ELSE
					INSERT INTO OutputList5 VALUES(record.qid, rank, record.AdvertiserId, record.balance5, record.budget);
			
				END IF;

				IF (record.ctcDisplayCount5 < 99) THEN
					UPDATE Advertisers
					SET ctcDisplayCount5 = ctcDisplayCount5 + 1
					WHERE Advertisers.AdvertiserId = record.Advertiserid;
				ELSE
					UPDATE Advertisers
					SET ctcDisplayCount5 = 0
					WHERE Advertisers.AdvertiserId = record.Advertiserid;
				END IF;
				rank := rank + 1;
			END LOOP;
			--DELETE FROM FullTable;
			rank := 1;
			advId := 0;
			
			FOR record IN (SELECT qid, AdvertiserId, budget, balance2, CTC, totalBid, ctcDisplayCount2, adRank2 FROM
							(SELECT qid, AdvertiserId, budget, balance2, CTC, totalBid, ctcDisplayCount2, adRank2 FROM FullTable
							 WHERE balance2 >= totalBid
							 ORDER BY adRank2 Desc, AdvertiserId Asc) WHERE ROWNUM <= k2)

			
			LOOP
				--SELECT ctcDisplayCount2 into advId from Advertisers WHERE Advertisers.AdvertiserId = record.AdvertiserId;
				IF (record.CTC*100 > record.ctcDisplayCount2) THEN
					
					SELECT max(totalBid)
					INTO nextBid FROM FullTable WHERE FullTable.totalBid IN (
					SELECT FullTable.totalBid FROM FullTable
					WHERE FullTable.totalBid < record.totalBid AND
					FullTable.balance2 >= FullTable.totalBid);
					IF nextBid IS NULL THEN
						nextBid := record.totalBid;
					END IF;

					INSERT INTO OutputList2 VALUES(record.qid, rank, record.AdvertiserId, record.balance2 - nextBid, record.budget);

					UPDATE Advertisers
					SET balance2 = balance2 - nextBid
					WHERE Advertisers.AdvertiserId = record.Advertiserid; 
				ELSE
					INSERT INTO OutputList2 VALUES(record.qid, rank, record.AdvertiserId, record.balance2, record.budget);
				END IF;

				IF (record.ctcDisplayCount2 < 99) THEN
					UPDATE Advertisers
					SET ctcDisplayCount2 = ctcDisplayCount2 + 1
					WHERE Advertisers.AdvertiserId = record.Advertiserid;
				ELSE
					UPDATE Advertisers
					SET ctcDisplayCount2 = 0
					WHERE Advertisers.AdvertiserId = record.Advertiserid;
				END IF;
				rank := rank + 1;
			END LOOP;
			--DELETE FROM FullTable;
			rank := 1;
			advId := 0;
			
			FOR record IN (SELECT qid, AdvertiserId, budget, balance4, CTC, totalBid, ctcDisplayCount4, adRank4 FROM
							(SELECT qid, AdvertiserId, budget, balance4, CTC, totalBid, ctcDisplayCount4, adRank4 FROM FullTable
							 WHERE balance4 >= totalBid
							 ORDER BY adRank4 Desc, AdvertiserId Asc) WHERE ROWNUM <= k4)

			
			LOOP
				IF (record.CTC*100 > record.ctcDisplayCount4) THEN
				SELECT max(totalBid)
					INTO nextBid FROM FullTable WHERE FullTable.totalBid IN (
					SELECT FullTable.totalBid FROM FullTable
					WHERE FullTable.totalBid < record.totalBid AND
					FullTable.balance4 >= FullTable.totalBid);
					IF nextBid IS NULL THEN
						nextBid := record.totalBid;
					END IF;

					INSERT INTO OutputList4 VALUES(record.qid, rank, record.AdvertiserId, record.balance4 - nextBid, record.budget);

					UPDATE Advertisers
					SET balance4 = balance4 - nextBid
					WHERE Advertisers.AdvertiserId = record.Advertiserid;

				ELSE
					INSERT INTO OutputList4 VALUES(record.qid, rank, record.AdvertiserId, record.balance4, record.budget);
				END IF;

				IF (record.ctcDisplayCount4 < 99) THEN
					UPDATE Advertisers
					SET ctcDisplayCount4 = ctcDisplayCount4 + 1
					WHERE Advertisers.AdvertiserId = record.Advertiserid;
				ELSE
					UPDATE Advertisers
					SET ctcDisplayCount4 = 0
					WHERE Advertisers.AdvertiserId = record.Advertiserid;
				END IF;
				rank := rank + 1;
			END LOOP;
			--DELETE FROM FullTable;
			rank := 1;
			advId := 0;
			
			FOR record IN (SELECT qid, AdvertiserId, budget, balance6, CTC, totalBid, ctcDisplayCount6, adRank6 FROM
							(SELECT qid, AdvertiserId, budget, balance6, CTC, totalBid, ctcDisplayCount6, adRank6 FROM FullTable
							 WHERE balance6 >= totalBid
							 ORDER BY adRank6 Desc, AdvertiserId Asc) WHERE ROWNUM <= k6)

			
			LOOP
				--SELECT ctcDisplayCount6 into advId from Advertisers WHERE Advertisers.AdvertiserId = record.AdvertiserId;
				IF (record.CTC*100 > record.ctcDisplayCount6) THEN
				SELECT max(totalBid)
					INTO nextBid FROM FullTable WHERE FullTable.totalBid IN (
					SELECT FullTable.totalBid FROM FullTable
					WHERE FullTable.totalBid < record.totalBid AND
					FullTable.balance6 >= FullTable.totalBid);
					IF nextBid IS NULL THEN
						nextBid := record.totalBid;
					END IF;

					INSERT INTO OutputList6 VALUES(record.qid, rank, record.AdvertiserId, record.balance6 - nextBid, record.budget);

					UPDATE Advertisers
					SET balance6 = balance6 - nextBid
					WHERE Advertisers.AdvertiserId = record.Advertiserid;

				ELSE
					INSERT INTO OutputList6 VALUES(record.qid, rank, record.AdvertiserId, record.balance6, record.budget);
				END IF;

				IF (record.ctcDisplayCount6 < 99) THEN
					UPDATE Advertisers
					SET ctcDisplayCount6 = ctcDisplayCount6 + 1
					WHERE Advertisers.AdvertiserId = record.Advertiserid;
				ELSE
					UPDATE Advertisers
					SET ctcDisplayCount6 = 0
					WHERE Advertisers.AdvertiserId = record.Advertiserid;
				END IF;
				rank := rank + 1;
			END LOOP;
			--DELETE FROM FullTable;
		COMMIT;
	END;
/

CREATE OR REPLACE PROCEDURE calculateRank (queryCount IN int, k1 IN int, k2 IN int, k3 IN int, k4 IN int, k5 IN int, k6 IN int)
IS
query NUMBER := 1;
	BEGIN
		LOOP
			tokenizesingle(query);
			adwords;
			display(query, k1, k2, k3, k4, k5, k6);
			query := query + 1;
			EXIT WHEN query = queryCount + 1;
		END LOOP;
		COMMIT;
	END;
/

exit;