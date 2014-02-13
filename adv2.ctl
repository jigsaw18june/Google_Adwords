LOAD DATA INFILE 
'Advertisers.dat'
into TABLE advertisers 
fields terminated by '\t'
TRAILING NULLCOLS
(advertiserId  , budget , ctc , balance1 ":budget", ctcDisplayCount1 constant 0, balance2 ":budget", ctcDisplayCount2 constant 0 
, balance3 ":budget", ctcDisplayCount3 constant 0,  balance4 ":budget", ctcDisplayCount4 constant 0,  balance5 ":budget", ctcDisplayCount5 constant 0,  balance6 ":budget", ctcDisplayCount6 constant 0 )
