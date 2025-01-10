import psycopg
from flask import Flask, render_template, request, jsonify

POSTGRES_CONNECTION = "host=sql-database dbname=postgres user=postgres password=supersecret port=5432"

app = Flask(__name__)


@app.route('/')
def home():
    return render_template('home.html')


@app.route('/inventory', methods=['GET', 'POST'])
def inventory():
    if request.method == 'GET':
        with psycopg.connect(POSTGRES_CONNECTION, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT retailer_id, retailer_name FROM retailers")
                retailers = [{'id': row[0], 'name': row[1]} for row in cur.fetchall()]
                cur.execute("SELECT product_id, product_name FROM products")
                products = [{'id': row[0], 'name': row[1]} for row in cur.fetchall()]
            return render_template('inventory.html', retailers=retailers, products=products)

    elif request.method == 'POST':
        filters = request.json
        retailer_ids = filters.get('retailers', [])
        product_ids = filters.get('products', [])

        query = """
            SELECT 
                r.retailer_name,
                p.product_name,
                i.quantity_on_hand,
                CASE 
                    WHEN i.quantity_on_hand <= 0 THEN 'Out of Stock'
                    WHEN i.quantity_on_hand <= i.reorder_level THEN 'Low Stock'
                    ELSE 'In Stock'
                END AS status
            FROM 
                inventory i
            JOIN 
                retailers r ON i.retailer_id = r.retailer_id
            JOIN 
                products p ON i.product_id = p.product_id
            WHERE 
                (%s::int[] IS NULL OR r.retailer_id = ANY(%s))
                AND (%s::int[] IS NULL OR p.product_id = ANY(%s))
        """

        with psycopg.connect(POSTGRES_CONNECTION, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute(query, (retailer_ids, retailer_ids, product_ids, product_ids))
                data = [
                    {
                        'retailer_name': row[0],
                        'product_name': row[1],
                        'quantity_on_hand': row[2],
                        'status': row[3]
                    } for row in cur.fetchall()
                ]
            return jsonify(data)


@app.route('/forecasting')
def forecasting():
    return render_template('forecasting.html')


if __name__ == '__main__':
    app.run(debug=True)

