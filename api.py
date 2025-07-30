# api.py (Memory Leak Fix)

# Load environment variables from .env file FIRST.
from dotenv import load_dotenv
load_dotenv()

# Standard library imports
import os
import json
from datetime import datetime, timedelta

# Flask and extensions
from flask import Flask, jsonify, g, request, render_template, redirect
from flask_cors import CORS
from flask_mail import Mail, Message
from flask_login import UserMixin, login_user, logout_user, login_required, current_user, LoginManager
from werkzeug.security import generate_password_hash, check_password_hash

# Third-party libraries
import stripe
import google.generativeai as genai
import psycopg2 
from psycopg2.extras import RealDictCursor
import torch
from sentence_transformers import SentenceTransformer, util

# --- FLASK APP SETUP ---
app = Flask(__name__, static_folder='static', template_folder='templates')
app.secret_key = os.environ.get("FLASK_SECRET_KEY", "a-strong-default-secret-key")
CORS(app, supports_credentials=True)

# --- GLOBAL VARIABLES FOR CACHING MODELS ---
# We will load the models into these variables the first time they are needed.
retriever = None
grant_ids = None
grant_embeddings = None

# --- DATABASE SETUP ---
def get_db():
    if 'db' not in g:
        g.db = psycopg2.connect(os.environ.get("DATABASE_URL"), cursor_factory=RealDictCursor)
    return g.db

@app.teardown_appcontext
def close_db(e=None):
    db = g.pop('db', None)
    if db is not None:
        db.close()

# --- FLASK-LOGIN SETUP ---
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login'

class User(UserMixin):
    def __init__(self, user_data):
        self.id = user_data['id']
        self.email = user_data['email']
        self.stripe_customer_id = user_data.get('stripe_customer_id')

@login_manager.user_loader
def load_user(user_id):
    db = get_db()
    cursor = db.cursor()
    cursor.execute("SELECT * FROM users WHERE id = %s", (user_id,))
    user_data = cursor.fetchone()
    if user_data:
        return User(user_data)
    return None

# --- WEBSITE ROUTES ---
# ... (Your existing routes: /dashboard, /login, /signup, /crm, etc.) ...
@app.route('/')
def index():
    return render_template('index.html')

@app.route('/dashboard')
@login_required
def dashboard():
    return render_template('dashboard.html', user_email=current_user.email)

@app.route('/login')
def login():
    return render_template('login.html')

@app.route('/signup')
def signup():
    return render_template('signup.html')

@app.route('/crm')
@login_required
def crm():
    return render_template('crm.html')

# --- API ENDPOINTS ---

# --- CORRECTED: AI MATCHING ENDPOINT ---
@app.route('/api/matches')
@login_required
def get_matches():
    global retriever, grant_ids, grant_embeddings

    # 1. Load models and data ONLY if they haven't been loaded yet
    if retriever is None:
        print("Loading AI models for the first time...")
        retriever = SentenceTransformer('all-MiniLM-L6-v2')
        try:
            with open('grant_embeddings.json', 'r') as f:
                embedding_data = json.load(f)
                grant_ids = embedding_data['grant_ids']
                grant_embeddings = torch.tensor(embedding_data['embeddings'])
            print("AI models and embeddings loaded successfully.")
        except FileNotFoundError:
            print("WARNING: grant_embeddings.json not found.")
            return jsonify(error="The grant embeddings file has not been generated yet. Please run the data pipeline."), 500

    # 2. Get the user's profile to find their mission
    db = get_db()
    cursor = db.cursor()
    cursor.execute("SELECT mission_statement FROM users WHERE id = %s", (current_user.id,))
    user_profile = cursor.fetchone()

    if not user_profile or not user_profile.get('mission_statement'):
        return jsonify(error="Your profile is incomplete. Please add your mission statement in the settings."), 400

    # 3. Perform the AI search
    query_embedding = retriever.encode(user_profile['mission_statement'], convert_to_tensor=True)
    cos_scores = util.cos_sim(query_embedding, grant_embeddings)[0]
    top_results = torch.topk(cos_scores, k=100) # Find top 100 grants

    # 4. Process and return the results
    matches = []
    for score, idx in zip(top_results[0], top_results[1]):
        grant_id = grant_ids[idx]
        cursor.execute("""
            SELECT g.grant_purpose, g.grant_amount, f.name as foundation_name, f.ein as foundation_ein
            FROM grants g
            JOIN foundations f ON g.foundation_ein = f.ein
            WHERE g.id = %s
        """, (grant_id,))
        match_data = cursor.fetchone()
        if match_data:
            matches.append({
                "score": score.item(),
                "grant": match_data
            })
    
    return jsonify(matches)


# --- (Your other existing API endpoints: /api/signup, /api/login, CRM endpoints, etc.) ---
@app.route('/api/signup', methods=['POST'])
def handle_signup():
    data = request.json
    email = data.get('email')
    password = data.get('password')
    if not email or not password:
        return jsonify(error="Email and password are required."), 400

    db = get_db()
    cursor = db.cursor()
    cursor.execute("SELECT id FROM users WHERE email = %s", (email,))
    if cursor.fetchone():
        return jsonify(error="An account with this email already exists."), 409

    hashed_password = generate_password_hash(password)
    cursor.execute("INSERT INTO users (email, password_hash) VALUES (%s, %s) RETURNING id, email", (email, hashed_password))
    new_user_data = cursor.fetchone()
    db.commit()
    
    user = User(new_user_data)
    login_user(user)
    return jsonify(message="Signup successful!", user={'id': user.id, 'email': user.email}), 201

@app.route('/api/login', methods=['POST'])
def handle_login():
    data = request.json
    email = data.get('email')
    password = data.get('password')

    db = get_db()
    cursor = db.cursor()
    cursor.execute("SELECT * FROM users WHERE email = %s", (email,))
    user_data = cursor.fetchone()

    if user_data and check_password_hash(user_data['password_hash'], password):
        user = User(user_data)
        login_user(user)
        return jsonify(message="Login successful!"), 200
    
    return jsonify(error="Invalid email or password."), 401

@app.route('/api/logout', methods=['POST'])
@login_required
def handle_logout():
    logout_user()
    return jsonify(message="Logout successful!"), 200

# --- MAIN APP LOGIC ---
if __name__ == '__main__':
    app.run(debug=True, port=5000)

