import os
import logging
import uuid
import threading
import shutil
from flask import Flask, render_template, jsonify, request, Response
from flask_cors import CORS
import pandas as pd

# Dependency Check
try:
    import openpyxl
except ImportError:
    print("\n" + "!"*70)
    print("CRITICAL MISSING DEPENDENCY: openpyxl")
    print("You must install openpyxl to allow writing to Excel files.")
    print("Run this command in your terminal: pip install openpyxl")
    print("!"*70 + "\n")

# ==========================================
# 1. APPLICATION CONFIGURATION & SETUP
# ==========================================
app = Flask(__name__)
CORS(app)  # Enable Cross-Origin Resource Sharing

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Database File Paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, 'data')
EXCEL_DB_PATH = os.path.join(DATA_DIR, 'Class_Performance_Teaching_Style.xlsx')

@app.after_request
def add_header(response):
    """CRITICAL FIX: Prevent browser caching to guarantee live data fetches."""
    response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response

# ==========================================
# 2. EXCEL DATABASE ENGINE (ATOMIC & CONCURRENT)
# ==========================================
class ExcelDatabase:
    """
    A thread-safe Database Engine that uses a Pandas DataFrame as RAM
    and an Excel file as the persistent storage layer with Atomic Saves.
    """
    def __init__(self, filepath):
        self.filepath = filepath
        
        # CRITICAL FIX: Pandas strictly enforces file extensions. 
        # Using ".tmp" caused the "No engine for filetype" error. 
        # We must use valid .xlsx extensions for our temporary files.
        base, ext = os.path.splitext(filepath)
        if not ext: ext = '.xlsx'
        self.tmp_filepath = f"{base}_TMP{ext}"
        self.bak_filepath = f"{base}_BAK{ext}"
        
        self.lock = threading.Lock()
        
        # Strict Schema Definition
        self.columns = [
            'Student_ID', 'Student_Name', 'Subject', 'Teaching_Style', 
            'Term', 'Score', 'Attendance_%', 'Performance_Category', 'Teacher_Name'
        ]
        self.df = pd.DataFrame(columns=self.columns)
        self._initialize_db()

    def _initialize_db(self):
        """Loads data from Excel or seeds default data if file is missing."""
        os.makedirs(os.path.dirname(self.filepath), exist_ok=True)
        if os.path.exists(self.filepath):
            try:
                self.df = pd.read_excel(self.filepath, sheet_name='Student_Performance', engine='openpyxl')
                self._enforce_schema()
                logging.info(f"Successfully loaded {len(self.df)} records from {os.path.basename(self.filepath)}")
            except Exception as e:
                logging.error(f"Error reading Excel DB: {e}. Checking for backup...")
                if os.path.exists(self.bak_filepath):
                    shutil.copy2(self.bak_filepath, self.filepath)
                    logging.info("Restored from backup. Rebooting DB...")
                    self._initialize_db()
                else:
                    self._seed_mock_data()
        else:
            self._seed_mock_data()

    def _enforce_schema(self):
        """Ensures all columns exist and forces strict data types to prevent corruption."""
        for col in self.columns:
            if col not in self.df.columns:
                self.df[col] = None
                
        # Drop rows that are completely empty/invalid
        self.df.dropna(subset=['Student_Name', 'Subject'], inplace=True)
                
        # Strip hidden spaces from strings to prevent mismatch bugs
        self.df['Student_Name'] = self.df['Student_Name'].astype(str).str.strip()
        self.df['Teacher_Name'] = self.df['Teacher_Name'].astype(str).str.strip()
        self.df['Subject'] = self.df['Subject'].astype(str).str.strip()

        # Enforce numeric data types
        self.df['Score'] = pd.to_numeric(self.df['Score'], errors='coerce').fillna(0.0)
        self.df['Attendance_%'] = pd.to_numeric(self.df['Attendance_%'], errors='coerce').fillna(85.0)

    def _seed_mock_data(self):
        """Seeds initial data if no Excel file exists."""
        logging.info("No Excel DB found. Generating seed data...")
        mock_data = [
            ("Alice Johnson", "Data Science", "Visual", "Term 1", 85.0, 92.0, "Mr. Patel"),
            ("Bob Smith", "Cryptography", "Blended", "Term 1", 91.0, 95.0, "Mr. Iyer"),
            ("Charlie Davis", "Data Science", "Visual", "Term 1", 45.0, 65.0, "Mr. Patel"),
        ]
        records = []
        for name, sub, style, term, score, att, teacher in mock_data:
            records.append({
                'Student_ID': uuid.uuid4().hex[:6].upper(), 'Student_Name': name,
                'Subject': sub, 'Teaching_Style': style, 'Term': term,
                'Score': score, 'Attendance_%': att,
                'Performance_Category': 'Average', 'Teacher_Name': teacher
            })
        self.df = pd.DataFrame(records, columns=self.columns)
        self.commit()

    def commit(self):
        """
        ATOMIC SAVE: Writes to a temporary file, creates a backup, 
        and safely replaces the original file. Outsmarts Windows File Locks.
        """
        with self.lock:
            try:
                # Dynamic check for openpyxl to provide clear error message
                try:
                    import openpyxl
                except ImportError:
                    return False, "Python module 'openpyxl' is missing. Run: pip install openpyxl"

                os.makedirs(os.path.dirname(self.filepath), exist_ok=True)
                
                # 1. Write to temporary file first 
                # (FIXED: Pandas requires the file extension to be .xlsx, which we fixed in __init__)
                self.df.to_excel(self.tmp_filepath, index=False, sheet_name='Student_Performance', engine='openpyxl')
                
                # 2. Try Atomic replace
                try:
                    if os.path.exists(self.filepath):
                        shutil.copy2(self.filepath, self.bak_filepath)
                    os.replace(self.tmp_filepath, self.filepath)
                    logging.info(f"Database saved to {os.path.basename(self.filepath)} successfully.")
                    return True, "Success"
                
                except (PermissionError, OSError) as e:
                    # 3. ANTI-LOCK FALLBACK: If user has Excel open, Windows blocks saving.
                    # We bypass the block by generating an Unlocked copy and redirecting the server to it!
                    logging.warning(f"File locked by Excel! Triggering bypass... ({e})")
                    
                    fallback_name = os.path.basename(self.filepath).replace('.xlsx', '_UNLOCKED.xlsx')
                    fallback_path = os.path.join(os.path.dirname(self.filepath), fallback_name)
                    
                    # (Explicitly setting engine here too for consistency)
                    self.df.to_excel(fallback_path, index=False, sheet_name='Student_Performance', engine='openpyxl')
                    
                    # Permanently redirect backend logic to the new safe file
                    self.filepath = fallback_path
                    f_base, f_ext = os.path.splitext(self.filepath)
                    self.tmp_filepath = f"{f_base}_TMP{f_ext}"
                    self.bak_filepath = f"{f_base}_BAK{f_ext}"
                    
                    logging.info(f"Successfully bypassed lock. Now syncing to: {fallback_name}")
                    return True, f"Excel file locked! Saved safely to {fallback_name} instead."
                    
            except Exception as e:
                err_msg = f"Failed to write to Excel: {e}"
                logging.error(err_msg)
                return False, err_msg

# Instantiate the global database engine
db = ExcelDatabase(EXCEL_DB_PATH)

# ==========================================
# 3. WRITABLE API ENDPOINTS (CRUD)
# ==========================================

@app.route('/api/add_student', methods=['POST'])
def add_student():
    """CREATE: Adds a new student and syncs to Excel."""
    data = request.json
    if not data or not data.get('name'):
        return jsonify({"status": "error", "message": "Invalid or missing student name"}), 400

    name = str(data.get('name')).strip()
    try:
        attendance = float(data.get('attendance', 85.0))
    except ValueError:
        attendance = 85.0
        
    scores = data.get('scores', {})
    student_id = "NEW-" + uuid.uuid4().hex[:4].upper()
    new_records = []
    
    for subject, score in scores.items():
        if score != '-':
            try:
                numeric_score = float(score)
            except ValueError:
                continue
                
            new_records.append({
                'Student_ID': student_id, 'Student_Name': name, 'Subject': str(subject).strip(),
                'Teaching_Style': "Blended", 'Term': "Term 1", 'Score': numeric_score,
                'Attendance_%': attendance, 'Performance_Category': "Average", 'Teacher_Name': "Unassigned"
            })
            
    if new_records:
        with db.lock:
            db.df = pd.concat([db.df, pd.DataFrame(new_records)], ignore_index=True)
            
        success, msg = db.commit()
        if not success:
            with db.lock:
                db.df = db.df[db.df['Student_ID'] != student_id]
            return jsonify({"status": "error", "message": msg}), 500
        
    return jsonify({"status": "success", "message": "Student inserted into Excel"}), 201


@app.route('/api/edit_scores', methods=['PUT'])
def edit_scores():
    """UPDATE: Modifies existing scores in Excel."""
    data = request.json
    student_name = str(data.get('student', '')).strip()
    scores = data.get('scores', {})
    
    if not student_name:
        return jsonify({"status": "error", "message": "Student name is required"}), 400
    
    with db.lock:
        df_backup = db.df.copy() # Save state for rollback
        for subject, new_score in scores.items():
            subject = str(subject).strip()
            mask = (db.df['Student_Name'] == student_name) & (db.df['Subject'] == subject)
            
            if new_score == '-' or new_score == "":
                db.df = db.df[~mask] # Delete score
            else:
                try:
                    num_score = float(new_score)
                    if mask.any():
                        db.df.loc[mask, 'Score'] = num_score
                    else:
                        new_row = pd.DataFrame([{
                            'Student_ID': f"MOD-{uuid.uuid4().hex[:4].upper()}", 'Student_Name': student_name, 'Subject': subject,
                            'Teaching_Style': "Blended", 'Term': "Term 1", 'Score': num_score,
                            'Attendance_%': 85.0, 'Performance_Category': "Average", 'Teacher_Name': "Unassigned"
                        }])
                        db.df = pd.concat([db.df, new_row], ignore_index=True)
                except ValueError:
                    continue

    success, msg = db.commit()
    if not success:
        with db.lock:
            db.df = df_backup # Rollback memory to match disk
        return jsonify({"status": "error", "message": msg}), 500
        
    return jsonify({"status": "success", "message": msg}), 200


@app.route('/api/edit_name', methods=['PUT'])
def edit_name():
    """UPDATE: Globally renames an entity across the Excel database."""
    data = request.json
    old_name = str(data.get('oldName', '')).strip()
    new_name = str(data.get('newName', '')).strip()
    entity_type = data.get('entityType')
    
    if not old_name or not new_name:
        return jsonify({"status": "error", "message": "Invalid name provided"}), 400
    
    with db.lock:
        df_backup = db.df.copy()
        if entity_type == 'Student':
            db.df.loc[db.df['Student_Name'] == old_name, 'Student_Name'] = new_name
        elif entity_type == 'Teacher':
            db.df.loc[db.df['Teacher_Name'] == old_name, 'Teacher_Name'] = new_name
            
    success, msg = db.commit()
    if not success:
        with db.lock:
            db.df = df_backup
        return jsonify({"status": "error", "message": msg}), 500
        
    return jsonify({"status": "success", "message": msg}), 200

# ==========================================
# 4. READ-ONLY ANALYTICS API ENDPOINTS
# ==========================================
@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/heatmap_data')
def get_heatmap_data():
    try:
        with db.lock:
            if db.df.empty: return jsonify({"subjects": [], "matrix": []})
            grouped = db.df.groupby(['Student_Name', 'Subject'])['Score'].mean().reset_index()
            
        subjects = sorted(grouped['Subject'].unique().tolist())
        pivot = grouped.pivot(index='Student_Name', columns='Subject', values='Score')
        
        matrix = []
        for student, row in pivot.iterrows():
            scores = {sub: round(float(row.get(sub)), 1) if pd.notna(row.get(sub)) else "-" for sub in subjects}
            matrix.append({"student": str(student), "scores": scores})

        return jsonify({"subjects": subjects, "matrix": matrix})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/student_analysis')
def get_student_analysis():
    try:
        with db.lock:
            if db.df.empty: return jsonify({"strong": [], "weak": [], "category_counts": {}})
            student_agg = db.df.groupby('Student_Name').agg({'Score': 'mean', 'Attendance_%': 'mean'}).reset_index()
            term_trend_data = db.df.groupby('Term')['Score'].mean().round(1).to_dict()

        strong, weak = [], []
        counts = {"High Performer": 0, "Average Performer": 0, "Needs Improvement": 0}

        for _, row in student_agg.iterrows():
            student = {"name": str(row['Student_Name']), "avg": round(row['Score'], 1), "attendance": round(row['Attendance_%'], 1)}
            if row['Score'] >= 80: strong.append(student); counts["High Performer"] += 1
            elif row['Score'] < 60: weak.append(student); counts["Needs Improvement"] += 1
            else: counts["Average Performer"] += 1

        return jsonify({"strong": sorted(strong, key=lambda x: x['avg'], reverse=True), "weak": sorted(weak, key=lambda x: x['avg']), "term_trend": term_trend_data, "category_counts": counts})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/teaching_style')
def get_teaching_style_analysis():
    try:
        with db.lock:
            if db.df.empty: return jsonify({"teacher_metrics": [], "style_averages": {}})
            teacher_agg = db.df.groupby(['Teacher_Name', 'Subject', 'Teaching_Style'])['Score'].mean().reset_index()
        
        results, style_totals, style_counts = [], {}, {}
        for _, row in teacher_agg.iterrows():
            avg_score, style = round(row['Score'], 1), str(row['Teaching_Style'])
            style_totals[style] = style_totals.get(style, 0) + avg_score
            style_counts[style] = style_counts.get(style, 0) + 1
            suggestion = f"Highly effective {style} approach!" if avg_score >= 80 else f"Consider blending other techniques with your {style} style."
            results.append({"teacher": str(row['Teacher_Name']), "subject": str(row['Subject']), "style": style, "overall_rating": avg_score, "suggestion": suggestion})
            
        style_averages = {k: round(v / style_counts[k], 1) for k, v in style_totals.items()}
        return jsonify({"teacher_metrics": sorted(results, key=lambda x: x['overall_rating'], reverse=True), "style_averages": style_averages})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/export')
def export_data():
    if not os.path.exists(db.filepath): return jsonify({"error": "Excel DB not found"}), 404
    with db.lock:
        with open(db.filepath, 'rb') as f: excel_data = f.read()
    return Response(excel_data, mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", headers={"Content-disposition": f"attachment; filename={os.path.basename(db.filepath)}"})

if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True, port=5000, threaded=True)