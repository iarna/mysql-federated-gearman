DROP TABLE IF EXISTS jobs;
CREATE TABLE jobs (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    command VARCHAR(200),
    args BLOB,
    status VARCHAR(20),
    result BLOB
)
ENGINE=FEDERATED
CHARSET=UTF8
CONNECTION='mysql://user@localhost:9306/database/table';
