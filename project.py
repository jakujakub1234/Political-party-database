import sqlite3
import sys
import json
from creator import create_database
from cipher import *
from pprint import pprint

initializing = False

opened = False

json_dict = dict()

seconds_in_year = 31536000

max_length_of_password = 128

cursor = None
connection = None

#Helpers:

def IsJson(probably_json):
  try:
    tester = json.loads(probably_json)
  except:
    return False
  return True

def ActionInitiator(action):
  global cursor
  
  cursor.execute("""SELECT initiator_id FROM action
  WHERE action_id=?""",(action,))
  members = cursor.fetchall()
  
  for member in members:
    return member['initiator_id']

def Initialize(name_of_database):
  create_database(name_of_database)

def CountVotes(action):
  global cursor
  
  cursor.execute("""SELECT count(*) FROM votes
  WHERE action_id=?
  and intention""",(action,))
  members = cursor.fetchall()

  for member in members:
    upvotes = member[0]

  cursor.execute("""SELECT count(*) FROM votes
  WHERE action_id=?
  and not(intention)""",(action,))
  members = cursor.fetchall()

def CorrectLeader(member,password):
  global cursor
  
  password = encode(password)

  cursor.execute("""SELECT * FROM member
  WHERE id=?
  AND password=?
  AND is_leader""",(member,password))
  members = cursor.fetchall()
  if(members):
    return True
  return False

#Creates:

def CreateLeader(timestamp, password, id):
  global cursor
  
  password = encode(password)

  cursor.execute("INSERT INTO member VALUES(?,1,?,?)",(id,password,timestamp))
  CreateTroll(id)

def CreateMember(timestamp, password, id):
  global cursor
  
  password = encode(password)

  cursor.execute("INSERT INTO member VALUES(?,0,?,?)",(id,password,timestamp))
  CreateTroll(id)

def CreateProject(project, authority):
  global cursor
  
  cursor.execute("INSERT INTO project VALUES(?,?)",(project, authority))

def CreateTroll(id):
  global cursor
  
  cursor.execute("INSERT INTO trollstatistics VALUES(?,0,0)",(id,))

def CreateUpvote(member,action,timestamp):
  global cursor
  
  cursor.execute("INSERT INTO votes VALUES(?,?,1,?)",(member,action,timestamp))

def CreateDownvote(member,action,timestamp):
  global cursor
  
  cursor.execute("INSERT INTO votes VALUES(?,?,0,?)",(member,action,timestamp))

def CreateAction(action, initiator, intention, timestamp, project):
  global cursor
  
  cursor.execute("INSERT INTO action VALUES(?,?,?,?,?)",(action, initiator, intention, timestamp, project))  

#Updates:

def IncrementTrollsUpvotes(member):
  global cursor
  
  cursor.execute("""UPDATE trollstatistics
  SET winning_actions = winning_actions+1
  WHERE member_id=?""",(member,))

def IncrementTrollsDownvotes(member):
  global cursor
  
  cursor.execute("""UPDATE trollstatistics
  SET lost_actions = lost_actions+1
  WHERE member_id=?""",(member,))

def DecrementTrollsUpvotes(member):
  global cursor
  
  cursor.execute("""UPDATE trollstatistics
  SET winning_actions = winning_actions-1
  WHERE member_id=?""",(member,))

def DecrementTrollsDownvotes(member):
  global cursor
  
  cursor.execute("""UPDATE trollstatistics
  SET lost_actions = lost_actions-1
  WHERE member_id=?""",(member,))

def UpdateTimestamp(member, timestamp):
  global cursor
  
  cursor.execute("""UPDATE member SET last_activity=?
  WHERE id=?""",(timestamp, member))

#Status prints:

def OkStatus():
  json_dict.clear()
  json_dict['status'] = 'OK'
  print(json_dict)
  
def ErrorStatus():
  json_dict.clear()
  json_dict['status'] = 'ERROR'
  print(json_dict)
  
#Selects:

def MemberExists(id): 
  global cursor
  
  cursor.execute("""SELECT * FROM member
  WHERE id=?""",(id,))
  members = cursor.fetchall()
  if(members):
    return True
  return False
  
def ActionExists(id):
  global cursor
  
  cursor.execute("""SELECT * FROM action
  WHERE action_id=?""",(id,))
  id_checker = cursor.fetchall()
  if(id_checker):
    return True
  return False
  
def ProjectExists(id):
  global cursor
  
  cursor.execute("""SELECT * FROM project
  WHERE project_id=?""",(id,))
  id_checker = cursor.fetchall()
  if(id_checker):
    return True
  return False
    
def AuthorityExists(id):
  global cursor
  
  cursor.execute("""SELECT * FROM project
  WHERE authority_id=?""",(id,))
  id_checker = cursor.fetchall()
  if(id_checker):
    return True
  return False
    
def IdExistst(id):
  if(MemberExists(id)):
    return True
    
  if(ActionExists(id)):
    return True
  
  if(ProjectExists(id)):
    return True
    
  if(AuthorityExists(id)):
    return True
    
  return False
  
def CantBeAuthority(id):
  if(MemberExists(id)):
    return True
    
  if(ActionExists(id)):
    return True
  
  if(ProjectExists(id)):
    return True
    
  return False

def CorrectPassword(id, password):
  global cursor
  
  password = encode(password)

  cursor.execute("""SELECT password FROM member
  WHERE id=?""",(id,))
  members = cursor.fetchall()

  for member in members:
    if(member['password'] == password):
      return True
      
  return False
  
def IsFrozen(id, timestamp):
  global cursor
  
  cursor.execute("""SELECT last_activity FROM member
  WHERE id=?""",(id,))
  members = cursor.fetchall()

  for member in members:
    if(timestamp - member['last_activity'] > seconds_in_year):
      return True 
  return False

def MemberAlreadyVoted(member,action):
  global cursor
  
  cursor.execute("""SELECT * FROM votes
  WHERE action_id=?
  and member_id=?""",(action,member))

  id_checker = cursor.fetchall()

  if(id_checker):
    return True
  return False



  for member in members:
    downvotes = member[0]

  return downvotes-upvotes

def PrintActions(actual_type, project, authority):
  global cursor
  
  json_dict.clear()
  json_dict['status'] = 'OK'

  cursor.execute("""SELECT action_id AS a, action.intention AS b, project_id AS c,
  authority_id AS d,
  COUNT(case when votes.intention=1 then 1 else NULL end) AS e,
  COUNT(case when votes.intention=0 then 1 else NULL end) AS f
  FROM action
  JOIN project USING(project_id)
  LEFT JOIN votes USING(action_id)
  WHERE action.intention like(?)
  AND project_id like(?)
  AND authority_id like(?)
  GROUP BY a
  ORDER BY a""",(actual_type,project,authority))

  printer = cursor.fetchall()

  json_dict['data'] = []

  for row in printer:
    json_dict['data'].append([row['a'], row['b'], row['c'], row['d'], row['e'], row['f']])

  print(json_dict)

def PrintProjects(authority):
  global cursor
  
  json_dict.clear()
  json_dict['status'] = 'OK'

  cursor.execute("""SELECT project_id AS a,
  authority_id AS b
  FROM project
  WHERE authority_id like(?)
  ORDER BY a""",(authority,))

  printer = cursor.fetchall()

  json_dict['data'] = []

  for row in printer:
    json_dict['data'].append([row['a'], row['b']])

  print(json_dict)

def PrintVotes(action, project):
  global cursor
  
  json_dict.clear()
  json_dict['status'] = 'OK'

  cursor.execute("""SELECT id AS a,
  COUNT(case when votes.intention=1 then 1 else NULL end) AS b,
  COUNT(case when votes.intention=0 then 1 else NULL end) AS c
  FROM member
  LEFT JOIN votes ON(id = member_id)
  LEFT JOIN action USING(action_id)
  WHERE (action_id like(?) OR action_id is NULL)
  AND (project_id like(?) OR project_id is NULL)
  GROUP BY a
  ORDER BY a""",(action,project))

  printer = cursor.fetchall()

  json_dict['data'] = []

  for row in printer:
    json_dict['data'].append([row['a'], row['b'], row['c']])

  print(json_dict)

def PrintTrolls(timestamp):
  global cursor
  global seconds_in_year

  json_dict.clear()
  json_dict['status'] = 'OK'

  timestamp -= seconds_in_year

  cursor.execute("""DROP TABLE IF EXISTS tmp""")

  cursor.execute("""CREATE TABLE tmp AS SELECT member_id AS a,
  winning_actions AS b,
  lost_actions AS c,
  last_activity AS d,
  'False' AS e
  FROM trollstatistics
  JOIN member ON(id=member_id)""")

  cursor.execute("""UPDATE tmp SET e='True'
  WHERE d>=?""",(timestamp,))

  cursor.execute("""SELECT a,b,c,d,e
  FROM tmp
  ORDER BY c-b, e""")

  printer = cursor.fetchall()

  json_dict['data'] = []

  for row in printer:
    json_dict['data'].append([row['a'], row['b'], row['c'], row['d'], row['e']])

  print(json_dict)

#Statements:

def ExitStatement():
  global opened
  global connection
  
  if(not(opened)):
    return
  connection.commit()
  
def OpenStatement(actual_json):
  global initializing
  global cursor
  global connection

  if(actual_json.get('login')=='init'):
    if(initializing):
      ErrorStatus()
      return
    
    name_of_database = actual_json.get('database')
    login = actual_json.get('login')
    password = actual_json.get('password')
    
    global max_length_of_password
    
    if(len(password) > max_length_of_password):
      ErrorStatus()
      return
    
    initializing = True
    
    Initialize(name_of_database)
    connection = sqlite3.connect(name_of_database +'.db')
    connection.row_factory = sqlite3.Row
    cursor = connection.cursor()
    
    OkStatus()      
    return
    
  else:
    if(initializing):
      ErrorStatus()
      return
    
    name_of_database = actual_json.get('database')
    connection = sqlite3.connect(name_of_database +'.db')
    connection.row_factory = sqlite3.Row
    cursor = connection.cursor()
    
    OkStatus()
  
def LeaderStatement(actual_json):
  global initializing
  global opened

  if(not(initializing) or not(opened)):
    ErrorStatus()
    return
    
  timestamp = actual_json.get('timestamp')
  password = actual_json.get('password')
  id = actual_json.get('member')
  
  global max_length_of_password
  
  if(len(password) > max_length_of_password or IdExistst(id)):
    ErrorStatus()
    return
  
  CreateLeader(timestamp, password, id)
  
  OkStatus()   
    
def SupportStatement(actual_json):
  global initializing
  global opened

  if(initializing or not(opened)):
    ErrorStatus()
    return
    
  timestamp = actual_json.get('timestamp')
  member = actual_json.get('member')
  password = actual_json.get('password')
  action = actual_json.get('action')
  project = actual_json.get('project')
  authority = actual_json.get('authority')
  
  global max_length_of_password
  
  if(not(MemberExists(member))):
    if(IdExistst(member) or len(password) > max_length_of_password):
      ErrorStatus()
      return
    CreateMember(timestamp, password, member)
    
  else:
    if(not(CorrectPassword(member,password)) or IsFrozen(member,timestamp)):
      ErrorStatus()
      return
    
  if(not(ProjectExists(project))):
    if(not(authority)):
      ErrorStatus()
      return

    else:
      if(IdExistst(project) or CantBeAuthority(authority)):
        ErrorStatus()
        return
      
      CreateProject(project, authority)

  if(IdExistst(action)):
    ErrorStatus()
    return
    
  UpdateTimestamp(member,timestamp)
  CreateAction(action, member, 1, timestamp, project)
  IncrementTrollsUpvotes(member)
  
  OkStatus()

def ProtestStatement(actual_json):
  global initializing
  global opened

  if(initializing or not(opened)):
    ErrorStatus()
    return
    
  timestamp = actual_json.get('timestamp')
  member = actual_json.get('member')
  password = actual_json.get('password')
  action = actual_json.get('action')
  project = actual_json.get('project')
  authority = actual_json.get('authority')
  
  global max_length_of_password
  
  if(not(MemberExists(member))):
    if(IdExistst(member) or len(password) > max_length_of_password):
      ErrorStatus()
      return
    CreateMember(timestamp, password, member)

  else:
    if(not(CorrectPassword(member,password)) or IsFrozen(member,timestamp)):
      ErrorStatus()
      return
  
  if(not(ProjectExists(project))):
    if(not(authority)):
      ErrorStatus()
      return

    else:
      if(IdExistst(project) or CantBeAuthority(authority)):
        ErrorStatus()
        return
      
      CreateProject(project, authority)
    
  if(IdExistst(action)):
    ErrorStatus()
    return
      
  UpdateTimestamp(member,timestamp)
  CreateAction(action, member, 0, timestamp, project)
  IncrementTrollsUpvotes(member)

  OkStatus()

def UpvoteStatement(actual_json):
  global initializing
  global opened

  if(initializing or not(opened)):
    ErrorStatus()
    return
    
  timestamp = actual_json.get('timestamp')
  member = actual_json.get('member')
  password = actual_json.get('password')
  action = actual_json.get('action')
  
  global max_length_of_password

  if(not(MemberExists(member))):
    if(IdExistst(member) or len(password) > max_length_of_password):
      ErrorStatus()
      return
    CreateMember(timestamp, password, member)
    
  else:
    if(not(CorrectPassword(member,password)) or IsFrozen(member,timestamp)):
      ErrorStatus()
      return
    
  if(not(ActionExists(action))):
    ErrorStatus()
    return

  if(MemberAlreadyVoted(member,action)):
    ErrorStatus()
    return

  CreateUpvote(member,action,timestamp)

  if(CountVotes(action) == 0):
    IncrementTrollsUpvotes(ActionInitiator(action))
    DecrementTrollsDownvotes(ActionInitiator(action))

  if(CountVotes(action) == 1):
    IncrementTrollsDownvotes(ActionInitiator(action))
    DecrementTrollsUpvotes(ActionInitiator(action))

  OkStatus()

def DownvoteStatement(actual_json):
  global initializing
  global opened

  if(initializing or not(opened)):
    ErrorStatus()
    return
    
  timestamp = actual_json.get('timestamp')
  member = actual_json.get('member')
  password = actual_json.get('password')
  action = actual_json.get('action')
  
  global max_length_of_password

  if(not(MemberExists(member))):
    if(IdExistst(member) or len(password) > max_length_of_password):
      ErrorStatus()
      return
    CreateMember(timestamp, password, member)
    
  else:
    if(not(CorrectPassword(member,password)) or IsFrozen(member,timestamp)):
      ErrorStatus()
      return
    
  if(not(ActionExists(action))):
    ErrorStatus()
    return

  if(MemberAlreadyVoted(member,action)):
    ErrorStatus()
    return

  CreateDownvote(member,action,timestamp)

  if(CountVotes(action) == 0):
    IncrementTrollsUpvotes(ActionInitiator(action))
    DecrementTrollsDownvotes(ActionInitiator(action))

  if(CountVotes(action) == 1):
    IncrementTrollsDownvotes(ActionInitiator(action))
    DecrementTrollsUpvotes(ActionInitiator(action))

  OkStatus()

def ActionsStatement(actual_json):
  global initializing
  global opened

  if(initializing or not(opened)):
    ErrorStatus()
    return
    
  timestamp = actual_json.get('timestamp')
  member = actual_json.get('member')
  password = actual_json.get('password')
  actual_type = actual_json.get('type')
  project = actual_json.get('project')
  authority = actual_json.get('authority')

  if(not(actual_type)):
    actual_type = '%'

  if(not(project)):
    project = '%'

  if(not(authority)):
    authority = '%'

  if(not(CorrectLeader(member,password))):
    ErrorStatus()
    return

  PrintActions(actual_type, project, authority)

def ProjectsStatement(actual_json):
  global initializing
  global opened

  if(initializing or not(opened)):
    ErrorStatus()
    return
    
  timestamp = actual_json.get('timestamp')
  member = actual_json.get('member')
  password = actual_json.get('password')
  authority = actual_json.get('authority')

  if(not(authority)):
    authority = '%'

  if(not(CorrectLeader(member,password))):
    ErrorStatus()
    return

  PrintProjects(authority) 

def VotesStatement(actual_json):
  global initializing
  global opened

  if(initializing or not(opened)):
    ErrorStatus()
    return
    
  timestamp = actual_json.get('timestamp')
  member = actual_json.get('member')
  password = actual_json.get('password')
  action = actual_json.get('action')
  project = actual_json.get('project')

  if(not(action)):
    action = '%'

  if(not(project)):
    project = '%'

  if(not(CorrectLeader(member,password))):
    ErrorStatus()
    return

  PrintVotes(action, project) 

def TrollsStatement(actual_json):
  global initializing
  global opened

  if(initializing or not(opened)):
    ErrorStatus()
    return
    
  timestamp = actual_json.get('timestamp')

  PrintTrolls(timestamp)

while(1):
  actual_query = input()
  
  if(actual_query == 'exit'):
    ExitStatement()
    break
    
  if(not(IsJson(actual_query))):
    #print('Invalid input!')
    continue
    
  actual_json = json.loads(actual_query)

  if(actual_json.get('open')):
    opened = True
    OpenStatement(actual_json.get('open'))
    continue
    
  if(actual_json.get('leader')):
    LeaderStatement(actual_json.get('leader'))
    continue
    
  if(actual_json.get('support')):
    SupportStatement(actual_json.get('support'))
    continue

  if(actual_json.get('protest')):
    ProtestStatement(actual_json.get('protest'))
    continue

  if(actual_json.get('upvote')):
    UpvoteStatement(actual_json.get('upvote'))
    continue

  if(actual_json.get('downvote')):
    DownvoteStatement(actual_json.get('downvote'))
    continue

  if(actual_json.get('actions')):
    ActionsStatement(actual_json.get('actions'))
    continue

  if(actual_json.get('projects')):
    ProjectsStatement(actual_json.get('projects'))
    continue

  if(actual_json.get('votes')):
    VotesStatement(actual_json.get('votes'))
    continue

  if(actual_json.get('trolls')):
    TrollsStatement(actual_json.get('trolls'))
    continue

#DEBUG PRINTS

'''
print("Members:")
print()
         
cursor.execute("""SELECT * FROM member""")
members = cursor.fetchall()
for member in members:
  print(member['id'], member['is_leader'], member['password'], member['last_activity'])

print()
print("Actions:")
print()

cursor.execute("""SELECT * FROM action""")
members = cursor.fetchall()
for member in members:
  print(member['action_id'], member['initiator_id'], member['intention'], member['creation_date'], member['project_id'])

print()
print("Trolls:")
print()

cursor.execute("""SELECT * FROM trollstatistics""")
members = cursor.fetchall()
for member in members:
  print(member['member_id'], member['winning_actions'], member['lost_actions'])
'''
print()
print("Votes:")
print()

cursor.execute("""SELECT * FROM votes""")
members = cursor.fetchall()
for member in members:
  print(member['member_id'], member['action_id'], member['intention'])

'''
print()
print("Projects:")
print()
cursor.execute("""SELECT * FROM project""")
members = cursor.fetchall()
for member in members:
  print(member['project_id'], member['authority_id'])

"""{ "open" :{ "database": "student", "login": "init", "password": "qwerty"}}"""
"""{ "leader" :{ "timestamp": 1557473000, "password": "abc", "member": "1"}}"""
"""{ "leader" :{ "timestamp": 1557474000, "password": "abc", "member": "2"}}"""
"""{ "support" :{ "timestamp": 1557474009, "password": "abc", "member": "2", "action": 500, "project":5000, "authority":10000}}"""  
"""{ "upvote" :{ "timestamp": 1557474009, "password": "abc", "member": "2", "action": 500}}"""
"""{ "actions" :{ "timestamp": 1557474009, "password": "abc", "member": "2", "type": 1, "project": 5000, "authority":10000}}"""
"""{ "votes" :{ "timestamp": 1557474009, "password": "abc", "member": "2", "action": 500, "project": 5000}}""" 
"""{ "projects" :{ "timestamp": 1557474009, "password": "abc", "member": "2", "authority": 10000}}"""  
"""{ "trolls" :{ "timestamp": 1557474009}}"""
'''