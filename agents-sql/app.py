from flask import Flask, request, jsonify, render_template
from openai import AzureOpenAI
import pyodbc
import os
from dotenv import load_dotenv
from datetime import datetime

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
# SQL CONFIG
# =====================================================
SQL_SERVER = os.getenv("SQL_SERVER")
SQL_DATABASE = os.getenv("SQL_DATABASE")
SQL_USER = os.getenv("SQL_USER")
SQL_PASSWORD = os.getenv("SQL_PASSWORD")

CONNECTION_STRING = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    f"SERVER={SQL_SERVER};"
    f"DATABASE={SQL_DATABASE};"
    f"UID={SQL_USER};"
    f"PWD={SQL_PASSWORD};"
    "Encrypt=yes;"
    "TrustServerCertificate=yes;"
    "Connection Timeout=30;"
)

def get_db_connection():
    return pyodbc.connect(CONNECTION_STRING)

# =====================================================
# DATA ACCESS
# =====================================================
def get_customer_context(customer_id):
    with get_db_connection() as conn:
        cursor = conn.cursor()

        cursor.execute("""
            SELECT c.FirstName, c.LastName, c.Email, c.Phone,
                COUNT(o.OrderID),
                SUM(o.TotalAmount)
            FROM Customers c
            LEFT JOIN Orders o ON c.CustomerID = o.CustomerID
            WHERE c.CustomerID = ?
            GROUP BY c.CustomerID, c.FirstName, c.LastName, c.Email, c.Phone
        """, customer_id)

        customer = cursor.fetchone()
        if not customer:
            return None

        cursor.execute("""
            SELECT TOP 5 o.OrderID, o.OrderDate, o.TotalAmount, o.Status,
                STRING_AGG(p.ProductName, ', ')
            FROM Orders o
            JOIN OrderItems oi ON o.OrderID = oi.OrderID
            JOIN Products p ON oi.ProductID = p.ProductID
            WHERE o.CustomerID = ?
            GROUP BY o.OrderID, o.OrderDate, o.TotalAmount, o.Status
            ORDER BY o.OrderDate DESC
        """, customer_id)

        orders = cursor.fetchall()

    return {
        "name": f"{customer[0]} {customer[1]}",
        "email": customer[2],
        "phone": customer[3],
        "total_orders": customer[4],
        "total_spent": float(customer[5]) if customer[5] else 0,
        "recent_orders": [
            {
                "order_id": o[0],
                "date": o[1].strftime("%Y-%m-%d"),
                "amount": float(o[2]),
                "status": o[3],
                "products": o[4]
            } for o in orders
        ]
    }

def save_conversation(customer_id, user_message, ai_response):
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO ConversationHistory (CustomerID, UserMessage, AIResponse)
            VALUES (?, ?, ?)
        """, customer_id, user_message, ai_response)
        conn.commit()

def get_conversation_history(customer_id, limit=5):
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT TOP (?) UserMessage, AIResponse, Timestamp
            FROM ConversationHistory
            WHERE CustomerID = ?
            ORDER BY Timestamp DESC
        """, limit, customer_id)
        rows = cursor.fetchall()

    return [
        {
            "user": r[0],
            "assistant": r[1],
            "timestamp": r[2].strftime("%Y-%m-%d %H:%M:%S")
        } for r in reversed(rows)
    ]

# =====================================================
# AI LOGIC
# =====================================================
def generate_ai_response(customer_id, user_message):
    customer = get_customer_context(customer_id)
    if not customer:
        return "Customer not found. Please check your customer ID."

    history = get_conversation_history(customer_id)

    system_prompt = f"""
You are a helpful customer support AI for an e-commerce company.

Customer:
- Name: {customer['name']}
- Email: {customer['email']}
- Phone: {customer['phone']}
- Orders: {customer['total_orders']}
- Total Spent: ${customer['total_spent']:.2f}

Recent Orders:
{chr(10).join([
    f"- Order #{o['order_id']}: {o['products']} (${o['amount']:.2f}, {o['status']}, {o['date']})"
    for o in customer['recent_orders']
])}
"""

    messages = [{"role": "system", "content": system_prompt}]

    for h in history:
        messages.append({"role": "user", "content": h["user"]})
        messages.append({"role": "assistant", "content": h["assistant"]})

    messages.append({"role": "user", "content": user_message})

    response = client.chat.completions.create(
        model=AZURE_DEPLOYMENT,
        messages=messages,
        temperature=0.7,
        max_tokens=500
    )

    ai_text = response.choices[0].message.content
    save_conversation(customer_id, user_message, ai_text)
    return ai_text

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
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT CustomerID, FirstName, LastName, Email FROM Customers")
        rows = cursor.fetchall()

    return jsonify([
        {"id": r[0], "name": f"{r[1]} {r[2]}", "email": r[3]}
        for r in rows
    ])

# =====================================================
# MAIN
# =====================================================
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)