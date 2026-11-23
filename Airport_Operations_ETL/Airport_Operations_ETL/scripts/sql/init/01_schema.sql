USE airport_etl;

CREATE TABLE Airport (
    AirportCode CHAR(3) PRIMARY KEY,
    AirportName VARCHAR(120) NOT NULL
) ENGINE=InnoDB;

CREATE TABLE Aircraft (
    AircraftID VARCHAR(10) PRIMARY KEY,
    Model VARCHAR(50) NOT NULL,
    Capacity INT NOT NULL CHECK (Capacity > 0)
) ENGINE=InnoDB;

CREATE TABLE Gate (
    GateID VARCHAR(10) PRIMARY KEY
) ENGINE=InnoDB;

CREATE TABLE Flight (
    FlightID VARCHAR(20) PRIMARY KEY,
    `Date` DATE NOT NULL,
    DepartureTime DATETIME NOT NULL,
    ActualDepartureTime DATETIME NULL,
    ArrivalTime DATETIME NOT NULL,
    `Status` VARCHAR(20) NOT NULL,
    DepartureAirportCode CHAR(3) NOT NULL,
    ArrivalAirportCode CHAR(3) NOT NULL,
    AircraftID VARCHAR(10) NOT NULL,
    GateID VARCHAR(10) NOT NULL,
    SourceUpdatedAt DATETIME NOT NULL,
    CHECK (`Status` IN ('Scheduled', 'Delayed', 'Departed', 'Arrived', 'Cancelled')),
    CHECK (DATE(DepartureTime) = `Date` AND ArrivalTime > DepartureTime),
    CHECK (DepartureAirportCode <> ArrivalAirportCode),
    CHECK ((`Status` IN ('Departed', 'Arrived')) = (ActualDepartureTime IS NOT NULL)),
    CHECK (ActualDepartureTime IS NULL OR ActualDepartureTime <= SourceUpdatedAt),
    FOREIGN KEY (DepartureAirportCode) REFERENCES Airport (AirportCode),
    FOREIGN KEY (ArrivalAirportCode) REFERENCES Airport (AirportCode),
    FOREIGN KEY (AircraftID) REFERENCES Aircraft (AircraftID),
    FOREIGN KEY (GateID) REFERENCES Gate (GateID),
    INDEX idx_flight_date (`Date`)
) ENGINE=InnoDB;

CREATE TABLE flight_raw (
    run_id VARCHAR(250) COLLATE utf8mb4_bin NOT NULL,
    source_line INT UNSIGNED NOT NULL,
    payload JSON NOT NULL,
    PRIMARY KEY (run_id, source_line)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE flight_clean (
    run_id VARCHAR(250) COLLATE utf8mb4_bin NOT NULL,
    FlightID VARCHAR(20) NOT NULL,
    `Date` DATE NOT NULL,
    DepartureTime DATETIME NOT NULL,
    ActualDepartureTime DATETIME NULL,
    ArrivalTime DATETIME NOT NULL,
    `Status` VARCHAR(20) NOT NULL,
    DepartureAirportCode CHAR(3) NOT NULL,
    ArrivalAirportCode CHAR(3) NOT NULL,
    AircraftID VARCHAR(10) NOT NULL,
    GateID VARCHAR(10) NOT NULL,
    SourceUpdatedAt DATETIME NOT NULL,
    PRIMARY KEY (run_id, FlightID)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE flight_reject (
    run_id VARCHAR(250) COLLATE utf8mb4_bin NOT NULL,
    source_line INT UNSIGNED NOT NULL,
    reason_code VARCHAR(40) NOT NULL,
    detail VARCHAR(500) NOT NULL,
    PRIMARY KEY (run_id, source_line)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE etl_log (
    log_id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    run_id VARCHAR(250) COLLATE utf8mb4_bin NOT NULL,
    step VARCHAR(20) NOT NULL,
    attempt INT UNSIGNED NOT NULL,
    outcome ENUM('STARTED', 'SUCCESS', 'FAILED') NOT NULL,
    metrics JSON NOT NULL,
    message VARCHAR(1000) NOT NULL,
    event_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    INDEX idx_log_run (run_id, log_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO Airport VALUES
    ('SGN', 'Tan Son Nhat International Airport'),
    ('HAN', 'Noi Bai International Airport'),
    ('DAD', 'Da Nang International Airport'),
    ('CXR', 'Cam Ranh Airport'),
    ('PQC', 'Phu Quoc International Airport');

INSERT INTO Aircraft VALUES
    ('AC001', 'Airbus A320', 180),
    ('AC004', 'Airbus A321', 200),
    ('AC006', 'Boeing 737-800', 189);

INSERT INTO Gate VALUES ('G01'), ('G02'), ('G03'), ('G04');

CREATE VIEW flight_departure_metrics AS
SELECT FlightID, `Date` AS service_date, DepartureAirportCode, ArrivalAirportCode,
       `Status`, DepartureTime, ActualDepartureTime,
       CASE WHEN ActualDepartureTime IS NULL THEN NULL
            ELSE ROUND(GREATEST(0, TIMESTAMPDIFF(SECOND, DepartureTime, ActualDepartureTime)) / 60.0, 2)
       END AS delay_minutes,
       CASE WHEN ActualDepartureTime IS NULL THEN NULL
            ELSE ActualDepartureTime > DepartureTime
       END AS is_delayed
FROM Flight;
