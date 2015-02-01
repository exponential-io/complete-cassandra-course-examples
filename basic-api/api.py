"""Basic REST API"""

from cassandra.cluster import Cluster
from cassandra import ConsistencyLevel
from flask import Flask, json, jsonify, request

app = Flask(__name__)

# Connect to the local Cassandra instance and use the 'accounts' keyspace
CLUSTER = Cluster(['127.0.0.1'])
SESSION = CLUSTER.connect()
SESSION.execute('USE accounts')

# Define a CQL prepared statement
INSERT_USER = SESSION.prepare("""
    INSERT INTO users (
        id,
        fname,
        lname,
        email)
    VALUES (?,?,?,?)
""")

INSERT_USER.consistency_level = ConsistencyLevel.ONE


GET_USER = SESSION.prepare("""
    SELECT
        id,
        fname,
        lname,
        email
    FROM users
    WHERE id = ?
""")

GET_USER.consistency_level = ConsistencyLevel.ONE


UPDATE_USER = SESSION.prepare("""
    UPDATE users
        SET fname = ?,
        lname = ?,
        email = ?
    WHERE id = ?
""")

UPDATE_USER.consistency_level = ConsistencyLevel.ONE


DELETE_USER = SESSION.prepare("""
    DELETE
    FROM users
    WHERE id = ?
""")

DELETE_USER.consistency_level = ConsistencyLevel.ONE


@app.route("/")
def hello():
    return "Basic API Exercise"


@app.route('/users', methods=['POST'])
def create_user():
    """Create a user"""
    input = request.get_json()
    result = SESSION.execute(INSERT_USER,
                             [input['user_id'],
                              input['fname'],
                              input['lname'],
                              input['email']])
    return jsonify(result="Created user")


@app.route('/users/<int:user_id>', methods=['GET'])
def read_user(user_id):
    """Get one user"""
    result = SESSION.execute(GET_USER, [user_id])[0]
    user = {'id': result.id,
            'fname': result.fname,
            'lname': result.lname,
            'email': result.email}
    return jsonify(user=user)


@app.route('/users/<int:user_id>', methods=['PUT'])
def update_user(user_id):
    """Update one user"""
    input = request.get_json()
    result = SESSION.execute(UPDATE_USER,
                             [input['fname'],
                              input['lname'],
                              input['email'],
                              user_id])
    return jsonify(result="Updated user")


@app.route('/users/<int:user_id>', methods=['DELETE'])
def delete_user(user_id):
    """Delete one user"""
    result = SESSION.execute(DELETE_USER, [user_id])
    return jsonify(result="Deleted user")


if __name__ == "__main__":
    app.run(debug=True)