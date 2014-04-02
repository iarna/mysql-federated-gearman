INSERT INTO jobs SET command='upper', args='this is a test';
SELECT * FROM jobs WHERE id=LAST_INSERT_ID();
