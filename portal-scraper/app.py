# app.py - Flask wrapper cho scraper + dashboard
from flask import Flask, jsonify, render_template
import os
import logging
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
from scraper import main as run_scraper

load_dotenv()

app = Flask(__name__)

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Database config
DB_CONFIG = {
    'host': os.getenv('DB_HOST', '35.198.243.191'),
    'port': int(os.getenv('DB_PORT', 5432)),
    'database': os.getenv('DB_NAME', 'scraper_data'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD')
}

def get_db_connection():
    """Kết nối PostgreSQL"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        logger.error(f"Database connection error: {e}")
        return None

# ============= SCRAPER ROUTES =============

@app.route('/scrape', methods=['GET', 'POST'])
def scrape():
    """Endpoint để Cloud Scheduler gọi - chạy scraper"""
    try:
        logger.info("🚀 Bắt đầu scraping từ Cloud Run...")
        
        # Chạy scraper
        run_scraper()
        
        return jsonify({
            "status": "success",
            "message": "Scraping completed successfully"
        }), 200
        
    except Exception as e:
        logger.error(f"Lỗi scraping: {e}")
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

@app.route('/health', methods=['GET'])
def health():
    """Health check endpoint"""
    return jsonify({"status": "healthy"}), 200

# ============= DASHBOARD ROUTES =============

@app.route('/', methods=['GET'])
def index():
    """Dashboard trang chủ - tổng quan"""
    try:
        conn = get_db_connection()
        if not conn:
            return "Database connection error", 500
        
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Tổng số môn học
            cur.execute("SELECT COUNT(*) as total FROM grades;")
            total_courses = cur.fetchone()['total']
            
            # Điểm trung bình
            cur.execute("SELECT AVG(total_score) as avg_score FROM grades WHERE total_score IS NOT NULL;")
            avg_score = cur.fetchone()['avg_score']
            
            # Số môn đạt
            cur.execute("SELECT COUNT(*) as passed FROM grades WHERE passed = true;")
            passed_count = cur.fetchone()['passed']
            
            # Số học kỳ
            cur.execute("SELECT COUNT(DISTINCT semester) as semesters FROM grades;")
            semesters = cur.fetchone()['semesters']
        
        conn.close()
        
        return render_template('index.html', 
                             total_courses=total_courses,
                             avg_score=round(avg_score, 2) if avg_score else 0,
                             passed_count=passed_count,
                             semesters=semesters)
    except Exception as e:
        logger.error(f"Error in index: {e}")
        return f"Error: {e}", 500

@app.route('/grades', methods=['GET'])
def grades_page():
    """Trang chi tiết bảng điểm"""
    return render_template('grades.html')

@app.route('/api/grades', methods=['GET'])
def api_grades():
    """API để lấy toàn bộ dữ liệu bảng điểm"""
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({'error': 'Database connection failed'}), 500
        
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT * FROM grades 
                ORDER BY semester DESC, course_code ASC
            """)
            grades = cur.fetchall()
        
        conn.close()
        return jsonify(grades)
    except Exception as e:
        logger.error(f"Error in api_grades: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/stats', methods=['GET'])
def api_stats():
    """API để lấy thống kê"""
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({'error': 'Database connection failed'}), 500
        
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Điểm theo học kỳ
            cur.execute("""
                SELECT semester, AVG(total_score) as avg_score, COUNT(*) as count
                FROM grades
                WHERE total_score IS NOT NULL
                GROUP BY semester
                ORDER BY semester DESC
            """)
            semester_stats = cur.fetchall()
            
            # Top 5 môn điểm cao
            cur.execute("""
                SELECT course_name, total_score, letter_grade
                FROM grades
                WHERE total_score IS NOT NULL
                ORDER BY total_score DESC
                LIMIT 5
            """)
            top_courses = cur.fetchall()
            
            # Phân bố điểm
            cur.execute("""
                SELECT letter_grade, COUNT(*) as count
                FROM grades
                WHERE letter_grade IS NOT NULL
                GROUP BY letter_grade
                ORDER BY letter_grade
            """)
            grade_distribution = cur.fetchall()
        
        conn.close()
        
        return jsonify({
            'semester_stats': semester_stats,
            'top_courses': top_courses,
            'grade_distribution': grade_distribution
        })
    except Exception as e:
        logger.error(f"Error in api_stats: {e}")
        return jsonify({'error': str(e)}), 500

# Root info endpoint (keep original)
@app.route('/info', methods=['GET'])
def info():
    """Info endpoint"""
    return jsonify({
        "service": "Portal UTH Scraper + Dashboard",
        "endpoints": {
            "/": "Dashboard (home)",
            "/grades": "Chi tiết bảng điểm",
            "/scrape": "Trigger scraping",
            "/health": "Health check",
            "/api/grades": "API - Lấy toàn bộ dữ liệu",
            "/api/stats": "API - Thống kê"
        }
    }), 200

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port, debug=False)