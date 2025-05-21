from flask import Flask, render_template, request, send_file, redirect, url_for, flash
import os
import pandas as pd
from analyze_votes import analyze_votes

app = Flask(__name__)
app.secret_key = "your_secret_key"
app.config['UPLOAD_FOLDER'] = 'uploads'
app.config['RESULT_FOLDER'] = 'results'

os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
os.makedirs(app.config['RESULT_FOLDER'], exist_ok=True)

@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        uploaded_file = request.files['file']
        category = request.form.get('category')

        if not uploaded_file or not category:
            flash("Please upload a file and select a category.")
            return redirect(url_for('index'))

        filepath = os.path.join(app.config['UPLOAD_FOLDER'], uploaded_file.filename)
        uploaded_file.save(filepath)

        output_file = os.path.join(app.config['RESULT_FOLDER'], f"{category[:50].replace(' ', '_')}_results.xlsx")

        try:
            analyze_votes(filepath, category, output_file)
            return send_file(output_file, as_attachment=True)
        except Exception as e:
            flash(f"Error processing file: {e}")
            return redirect(url_for('index'))

    return render_template('index.html')

@app.route('/categories', methods=['POST'])
def get_categories():
    uploaded_file = request.files['file']
    if uploaded_file:
        df = pd.read_excel(uploaded_file, engine='openpyxl', header=0)
        df.columns = df.columns.str.strip().str.lower()
        categories = [col for col in df.columns if "ip address" not in col.lower()]
        return {"categories": categories}
    return {"categories": []}

if __name__ == '__main__':
    app.run(debug=True)