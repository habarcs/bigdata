import pandas as pd
import plotly.express as px
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
            return render_template('inventory.html', retailers=retailers, products=products,
                                   inventory_data=inventory_data)

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


def graphing(title, table, graph_title, graph_label):
    with psycopg.connect(POSTGRES_CONNECTION, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT retailer_id, retailer_name FROM retailers")
            retailers = [{'id': row[0], 'name': row[1]} for row in cur.fetchall()]

            cur.execute("SELECT product_id, product_name FROM products")
            products = [{'id': row[0], 'name': row[1]} for row in cur.fetchall()]

    if request.method == 'GET':
        return render_template('graph.html', title=title, retailers=retailers,
                               products=products)

    elif request.method == 'POST':
        retailer_id = request.form['retailer_id']
        product_id = request.form['product_id']

        with psycopg.connect(POSTGRES_CONNECTION, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT retailer_name
                    FROM retailers
                    WHERE retailer_id = %s
                """, (retailer_id,))
                retailer_row = cur.fetchone()
                retailer_name = retailer_row[0] if retailer_row else None

                cur.execute("""
                    SELECT product_name
                    FROM products
                    WHERE product_id = %s
                """, (product_id,))
                product_row = cur.fetchone()
                product_name = product_row[0] if product_row else None

                cur.execute(f"""
                    SELECT ds, item_quantity
                    FROM {table}
                    WHERE retailer_id = %s AND product_id = %s
                    ORDER BY ds
                """, (retailer_id, product_id))
                data = cur.fetchall()

        if not data:
            error = "There is no sufficient data to make a demand prediction."
            return render_template('graph.html', title=title,
                                   retailers=retailers, products=products, error=error)

        df = pd.DataFrame(data, columns=['ds', 'item_quantity'])
        df['ds'] = pd.to_datetime(df['ds'])

        fig = px.line(df, x='ds', y='item_quantity',
                      title=f'{graph_title} <b>{product_name}</b> at Retailer <b>{retailer_name}</b>',
                      labels={'ds': 'Date', 'item_quantity': graph_label},
                      template='plotly_white')
        graph_html = fig.to_html(full_html=False)

        return render_template('graph.html', title=title,
                               graph_html=graph_html, retailers=retailers, products=products)


@app.route('/forecasting', methods=['GET', 'POST'])
def forecasting():
    title = "Demand Forecasting"
    database = "forecast"
    graph_title = "Demand Forecast for Product"
    graph_label = "Predicted Demand"
    return graphing(title, database, graph_title, graph_label)


@app.route('/history', methods=['GET', 'POST'])
def history():
    title = "Historical Demand"
    database = "historic_demand"
    graph_title = "Historical Demand for Product"
    graph_label = "Historical Demand"
    return graphing(title, database, graph_title, graph_label)


if __name__ == '__main__':
    app.run(debug=True)
