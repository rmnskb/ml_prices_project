-- DROP TABLE IF EXISTS flats;
-- DROP TABLE IF EXISTS flats_data;
-- DROP TABLE IF EXISTS flats_geo;
-- DROP TABLE IF EXISTS flats_poi;
-- DROP TABLE IF EXISTS flats_poi_junction;

CREATE TABLE flats (
    id BIGINT NOT NULL,
    name VARCHAR(200),
    price NUMERIC(10, 2),
    type INTEGER,
    checked_flg BOOLEAN,
    scrape_dt DATE DEFAULT CURRENT_DATE,
    PRIMARY KEY(id),
    UNIQUE(id)
);

CREATE TABLE flats_data (
    flat_id BIGINT NOT NULL,
    usable_area BIGINT,
    rooms BIGINT,
    furnished INT,
    parking_lot INT,
    terrace INT,
    balcony INT,
    loggia INT,
    elevator INT,
    cellar INT,
    basin INT,
    low_energy INT,
    easy_access INT,
    building_condition INT,
    garage INT,
    price NUMERIC(10, 2),
    price_note VARCHAR(255),
    update_dt DATE DEFAULT CURRENT_DATE,
    structure_type VARCHAR(255),
    floor VARCHAR(255),
    estate_state VARCHAR(255),
    PRIMARY KEY(flat_id),
    FOREIGN KEY(flat_id) REFERENCES flats(id)
);

CREATE TABLE flats_geo (
    flat_id BIGINT NOT NULL,
    longitude NUMERIC(12, 10),
    latitude NUMERIC(12, 10),
    street VARCHAR(255),
    house_num VARCHAR(255),
    postal_cd VARCHAR(255),
    district VARCHAR(255),
    city VARCHAR(255),
    county VARCHAR(255),
    state VARCHAR(255),
    country VARCHAR(255),
    PRIMARY KEY(flat_id),
    FOREIGN KEY (flat_id) REFERENCES flats(id)
);

CREATE TABLE flats_poi (
    poi_id BIGINT NOT NULL,
    name VARCHAR(255),
    distance BIGINT,
    rating INT,
    description VARCHAR(255),
    review_count BIGINT,
    PRIMARY KEY(poi_id)
);

CREATE TABLE flats_poi_junction (
    poi_id BIGINT NOT NULL,
    flat_id BIGINT NOT NULL,
    FOREIGN KEY (poi_id) REFERENCES flats_poi(poi_id),
    FOREIGN KEY (flat_id) REFERENCES flats(id)
);