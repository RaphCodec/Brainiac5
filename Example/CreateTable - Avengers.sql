CREATE TABLE [Avengers] (
    [URL] VARCHAR(77) NOT NULL,
    [Name/Alias] VARCHAR(45),
    [Appearances] [SMALLINT],
    [Current?] BIT,
    [Gender] VARCHAR(16),
    [Probationary Introl] VARCHAR(16),
    [Full/Reserve Avengers Intro] VARCHAR(16),
    [Year] [SMALLINT],
    [Years since joining] [TINYINT],
    [Honorary] VARCHAR(22),
    [Death1] VARCHAR(13),
    [Return1] VARCHAR(13),
    [Death2] VARCHAR(13),
    [Return2] VARCHAR(13),
    [Death3] VARCHAR(13),
    [Return3] VARCHAR(13),
    [Death4] VARCHAR(13),
    [Return4] VARCHAR(13),
    [Death5] VARCHAR(13),
    [Return5] VARCHAR(13),
    [Notes] VARCHAR(265)
);
ALTER TABLE [Avengers]
ADD CONSTRAINT [PK__Avengers__ID] PRIMARY KEY ([URL]);
