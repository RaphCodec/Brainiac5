CREATE TABLE [Avengers] (
    [URL] VARCHAR(67) NOT NULL,
    [Name/Alias] VARCHAR(35),
    [Appearances] [SMALLINT],
    [Current?] BIT,
    [Gender] VARCHAR(6),
    [Probationary Introl] VARCHAR(6),
    [Full/Reserve Avengers Intro] VARCHAR(6),
    [Year] [SMALLINT],
    [Years since joining] [TINYINT],
    [Honorary] VARCHAR(12),
    [Death1] VARCHAR(3),
    [Return1] VARCHAR(3),
    [Death2] VARCHAR(3),
    [Return2] VARCHAR(3),
    [Death3] VARCHAR(3),
    [Return3] VARCHAR(3),
    [Death4] VARCHAR(3),
    [Return4] VARCHAR(3),
    [Death5] VARCHAR(3),
    [Return5] VARCHAR(3),
    [Notes] VARCHAR(255)
);
ALTER TABLE [Avengers]
ADD CONSTRAINT [PK__Avengers__ID] PRIMARY KEY CLUSTERED ([URL]);
