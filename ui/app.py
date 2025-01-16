import psycopg
import plotly.express as px
import pandas as pd
from flask import Flask, render_template, request, jsonify

POSTGRES_CONNECTION = "host=localhost dbname=postgres user=postgres password=supersecret port=5432"

app = Flask(__name__)


@app.route('/')
def home():
    return render_template('home.html')


@app.route('/inventory', methods=['GET', 'POST'])
def inventory():
    if request.method == 'GET':
        with psycopg.connect(POSTGRES_CONNECTION, autocommit=True) as conn:
            with conn.cursor() as cur:
                # Fetch retailers and products
                cur.execute("SELECT retailer_id, retailer_name FROM retailers")
                retailers = [{'id': row[0], 'name': row[1]} for row in cur.fetchall()]
                cur.execute("SELECT product_id, product_name FROM products")
                products = [{'id': row[0], 'name': row[1]} for row in cur.fetchall()]
                
                # Fetch the first 10 inventory items
                cur.execute("""
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
                    LIMIT 10
                """)
                inventory_data = [
                    {
                        'retailer_name': row[0],
                        'product_name': row[1],
                        'quantity_on_hand': row[2],
                        'status': row[3]
                    } for row in cur.fetchall()
                ]
            return render_template('inventory.html', retailers=retailers, products=products, inventory_data=inventory_data)

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



@app.route('/forecasting', methods=['GET', 'POST'])
def forecasting():
    with psycopg.connect(POSTGRES_CONNECTION, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT retailer_id FROM forecast")
            retailers = [{'id': row[0]} for row in cur.fetchall()]
            cur.execute("SELECT product_id FROM forecast")
            products = [{'id': row[0]} for row in cur.fetchall()]

    if request.method == 'GET':
        return render_template('forecasting.html', retailers=retailers, products=products)

    elif request.method == 'POST':
        # Fetch selected retailer and product IDs from the form
        retailer_id = request.form['retailer_id']
        product_id = request.form['product_id']

        # Query the forecast table for the selected retailer and product
        with psycopg.connect(POSTGRES_CONNECTION, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT ds, item_quantity
                    FROM forecast
                    WHERE retailer_id = %s AND product_id = %s
                    ORDER BY ds
                """, (retailer_id, product_id))
                data = cur.fetchall()

        # Convert the data to a Pandas DataFrame
        df = pd.DataFrame(data, columns=['ds', 'item_quantity'])

        # Generate the interactive plot
        fig = px.line(df, x='ds', y='item_quantity', 
                      title=f'Demand Forecast for Product <b>{product_id}</b> at Retailer <b>{retailer_id}</b>',
                      labels={'ds': 'Date', 'item_quantity': 'Predicted Demand'},
                      template='plotly_white')
        graph_html = fig.to_html(full_html=False)

        # Return the plot in the response
        return render_template('forecasting.html', graph_html=graph_html, retailers=retailers, products=products)


if __name__ == '__main__':
    app.run(debug=True)

