from flask import Flask, render_template, g
import pyodbc

app = Flask("Sport App")

# Configuration for your database
DATABASE_CONFIG = {
    'driver': 'ODBC Driver 18 for SQL Server',
    'server': 'localhost',
    'port' : '1433',
    'database': 'sportScoreDB',
    'uid': 'sa',
    'pwd': 'reallyStrongPwd123'
}

def get_db_connection():
  if 'db' not in g:
    g.db = pyodbc.connect(f"DRIVER={DATABASE_CONFIG['driver']};\
                SERVER={DATABASE_CONFIG['server']}; \
                PORT={DATABASE_CONFIG['port']}; \
                DATABASE={DATABASE_CONFIG['database']}; \
                UID={DATABASE_CONFIG['uid']}; \
                PWD={DATABASE_CONFIG['pwd']}; \
                TrustServerCertificate=yes")
  return g.db

@app.teardown_appcontext
def close_db_connection(exception):
    db = g.pop('db', None)
    if db is not None:
        db.close()

@app.route('/')
def index():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT GameID, TournamentName, \
                   HomeTeamName, HomeTeamID, \
                   AwayTeamName, AwayTeamID, \
                   homeScore, awayScore FROM dbo.ViewGames")
    games = cursor.fetchall()
    return render_template('index.html', games=games)

@app.route('/team/<int:team_id>')
def team(team_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM Teams WHERE TeamID = ?", team_id)
    team = cursor.fetchone()
    if team is None:
        return "Team not found", 404
    return render_template('team.html', team=team)

if __name__ == '__main__':
    app.run(debug=True)