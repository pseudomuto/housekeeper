CREATE TABLE `logs` (
    `timestamp` DateTime,
    `level`     String,
    `message`   String
)
ENGINE = Log();
