from flask import Flask, request, jsonify, render_template
from openai import AzureOpenAI
import psycopg2
import os
import asyncio
from dotenv import load_dotenv
from datetime import datetime
from database import DatabaseFactory

# =====================================================
# INIT
# =====================================================
load_dotenv()
app = Flask(__name__)

# =====================================================
# AZURE OPENAI CONFIG
# =====================================================
client = AzureOpenAI(
    azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
    api_key=os.getenv("AZURE_OPENAI_API_KEY"),
    api_version="2024-12-01-preview"
)

AZURE_DEPLOYMENT = os.getenv("AZURE_OPENAI_DEPLOYMENT")

# =====================================================
# POSTGRES CONFIG
# =====================================================
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_DATABASE = os.getenv("POSTGRES_DATABASE")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")


def get_pg_connection():
    return psycopg2.connect(
        host=POSTGRES_HOST,
        dbname=POSTGRES_DATABASE,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        port=5432,
        sslmode="require",
    )

# =====================================================
# =====================================================
# AI LOGIC
# =====================================================
def generate_ai_response(customer_id, user_message):
    """Generate AI response using Azure OpenAI with customer context"""
    
    # Get database type from environment
    db_type = os.getenv("DB_TYPE", "postgresql")
    
    # Create appropriate database adapter
    db = DatabaseFactory.create_adapter(db_type)
    
    # Get customer context from database
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    customer_context = loop.run_until_complete(db.get_customer_context(customer_id))
    
    if not customer_context:
        return "I couldn't find your customer information. Please verify your customer ID."
    
    # Get conversation history
    conversation_history = loop.run_until_complete(db.get_conversation_history(customer_id))
    
    # Build system message with customer context
    system_message = f"""You are a helpful customer service AI assistant for an e-commerce company.
    
Customer Information:
- Name: {customer_context['name']}
- Email: {customer_context['email']}
- Phone: {customer_context['phone']}
- Total Orders: {customer_context['total_orders']}
- Total Spent: ${customer_context['total_spent']:.2f}

Recent Orders:
{chr(10).join([f"- Order #{order['order_id']}: {order['products']} (${order['amount']:.2f}, Status: {order['status']}, Date: {order['date']})" 
              for order in customer_context['recent_orders']])}

Provide helpful, personalized responses based on this customer's history. Be friendly and professional."""
    
    # Build messages for API call
    messages = [{"role": "system", "content": system_message}]
    
    # Add conversation history
    for entry in conversation_history:
        messages.append({"role": "user", "content": entry['user']})
        messages.append({"role": "assistant", "content": entry['assistant']})
    
    # Add current message
    messages.append({"role": "user", "content": user_message})
    
    # Call Azure OpenAI
    response = client.chat.completions.create(
        model=os.getenv("AZURE_OPENAI_DEPLOYMENT"),
        messages=messages,
        temperature=0.7,
        max_tokens=500
    )
    
    ai_response = response.choices[0].message.content
    
    # Save conversation to database
    loop.run_until_complete(db.store_conversation(customer_id, user_message, ai_response))
    
    return ai_response

# =====================================================
# ROUTES
# =====================================================
@app.route("/")
def index():
    return render_template("index.html")

@app.route("/api/chat", methods=["POST"])
def chat():
    data = request.json
    if not data.get("customer_id") or not data.get("message"):
        return jsonify({"error": "customer_id and message required"}), 400

    try:
        reply = generate_ai_response(data["customer_id"], data["message"])
        return jsonify({"response": reply})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/customers")
def customers():
    try:
        with get_pg_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT customer_id, first_name, last_name, email FROM customers")
            rows = cursor.fetchall()

        return jsonify([{ "id": r[0], "name": f"{r[1]} {r[2]}", "email": r[3]} for r in rows])
    except Exception as e:
        # Surface DB errors to help diagnose
        return jsonify({"error": str(e)}), 500

# =====================================================
# MAIN
# =====================================================
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)