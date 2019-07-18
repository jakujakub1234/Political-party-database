import sqlite3

def create_database(name_of_database):

  connection = sqlite3.connect(name_of_database + '.db')

  connection.row_factory = sqlite3.Row

  cursor = connection.cursor()

  cursor.executescript("""DROP TABLE IF EXISTS member;
  DROP TABLE IF EXISTS TrollStatistics;
  DROP TABLE IF EXISTS Action;
  DROP TABLE IF EXISTS VotesDetails;
  DROP TABLE IF EXISTS Votes;
  DROP TABLE IF EXISTS Project;""")

  cursor.executescript(""" CREATE TABLE member(
  id numeric PRIMARY KEY,
  is_leader boolean NOT NULL,
  password varchar (128) NOT NULL,
  last_activity timestamp NOT NULL);

  CREATE TABLE TrollStatistics(
  member_id numeric PRIMARY KEY,
  winning_actions numeric DEFAULT 0,
  lost_actions numeric DEFAULT 0);

  CREATE TABLE Action(
  action_id numeric PRIMARY KEY,
  initiator_id numeric NOT NULL,
  intention boolean NOT NULL,
  creation_date timestamp NOT NULL,
  project_id numeric NOT NULL);

  CREATE TABLE Votes(
  member_id numeric NOT NULL,
  action_id numeric NOT NULL,
  intention boolean NOT NULL,
  creation_date timestamp NOT NULL);

  CREATE TABLE Project(
  project_id numeric PRIMARY KEY,
  authority_id numeric NOT NULL); """)