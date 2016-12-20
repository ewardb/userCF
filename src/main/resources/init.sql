use movie;
#MovieID::Title::Genres
CREATE TABLE movies (
    id INTEGER NOT NULL AUTO_INCREMENT,
    title varchar(100) NOT NULL,
    published_year varchar(4) default NULL,
    type varchar(100) default NULL,
--    ...more movie information...
    PRIMARY KEY (id)
);
#UserID::Gender::Age::Occupation::Zip-code
CREATE TABLE users (
    id INTEGER NOT NULL AUTO_INCREMENT,
    name varchar(50) NOT NULL,
    email varchar(100) default NULL,
    gender varchar(2) NOT NULL,
    age Integer NOT NULL,
    occupation varchar(2) default NULL,
    zipCode varchar(6) default NULL,
--    ...more user information...
    PRIMARY KEY (id)
);


CREATE TABLE ratings (
    userID INTEGER NOT NULL,
    movieID INTEGER NOT NULL,
    preference INTEGER NOT NULL DEFAULT 0,
    timestamp INTEGER not null default 0,
    FOREIGN KEY (userID) REFERENCES users(id) ON DELETE CASCADE,
    FOREIGN KEY (movieID) REFERENCES movies(id) ON DELETE CASCADE
);

CREATE TABLE movie_similarity (
    movieID1 INTEGER NOT NULL,
    movieID2 INTEGER NOT NULL,
    similarity DOUBLE NOT NULL DEFAULT 0,
    FOREIGN KEY (movieID1) REFERENCES movies(id) ON DELETE CASCADE,
    FOREIGN KEY (movieID2) REFERENCES movies(id) ON DELETE CASCADE
);

CREATE INDEX ratings_index1 ON ratings ( userID , movieID );
CREATE INDEX ratings_index2 ON ratings ( userID );
CREATE INDEX ratings_index3 ON ratings ( movieID );
