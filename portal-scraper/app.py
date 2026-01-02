# app.py - Flask wrapper cho scraper + dashboard
from flask import Flask, jsonify, render_template
import os
import logging
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
from scraper import main as run_scraper

# Load env
load_dotenv()

app = Flask(__name__)

# Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Database config
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "35.198.243.191"),
    "port": int(os.getenv("DB_PORT", 5432)),
    "database": os.getenv("DB_NAME", "scraper_data"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD"),
}

def get_db_connection():
    try:
        return psycopg2.connect(**DB_CONFIG)
    except Exception as e:
        logger.error(f"Database connection error: {e}")
        return None


# ================= SCRAPER =================

@app.route("/scrape", methods=["GET", "POST"])
def scrape():
    """Trigger scraper (Cloud Scheduler / manual)"""
    try:
        logger.info("🚀 Start scraping...")
        run_scraper()
        return jsonify({"status": "success", "message": "Scraping completed"}), 200
    except Exception as e:
        logger.error(f"Scrape error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "healthy"}), 200


# ================= DASHBOARD =================

@app.route("/", methods=["GET"])
def index():
    """Dashboard trang chủ"""
    try:
        conn = get_db_connection()
        if not conn:
            return "Database connection error", 500

        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT COUNT(*) AS total FROM grades;")
            total_courses = cur.fetchone()["total"]

            # ✅ SỬA Ở ĐÂY – tính TBC theo tín chỉ
            cur.execute("""
                SELECT
                    SUM(total_score * credits) / NULLIF(SUM(credits), 0) AS avg_score
                FROM grades
                WHERE total_score IS NOT NULL
                  AND credits IS NOT NULL;
            """)
            avg_score = cur.fetchone()["avg_score"]

            cur.execute("SELECT COUNT(*) AS passed FROM grades WHERE passed = true;")
            passed_count = cur.fetchone()["passed"]

            cur.execute("SELECT COUNT(DISTINCT semester) AS semesters FROM grades;")
            semesters = cur.fetchone()["semesters"]

        conn.close()

        return render_template(
            "index.html",
            total_courses=total_courses,
            avg_score=round(avg_score, 2) if avg_score else 0,
            passed_count=passed_count,
            semesters=semesters
        )


    except Exception as e:
        logger.error(f"Index error: {e}")
        return f"Error: {e}", 500


@app.route("/grades", methods=["GET"])
def grades_page():
    """Trang chi tiết bảng điểm"""
    return render_template("grades.html")


# ================= API =================

@app.route("/api/grades", methods=["GET"])
def api_grades():
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({"error": "Database connection failed"}), 500

        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT *
                FROM grades
                ORDER BY semester DESC, course_code ASC;
            """)
            data = cur.fetchall()

        conn.close()
        return jsonify(data)

    except Exception as e:
        logger.error(f"API grades error: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/stats", methods=["GET"])
def api_stats():
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({"error": "Database connection failed"}), 500

        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT semester, AVG(total_score) AS avg_score, COUNT(*) AS count
                FROM grades
                WHERE total_score IS NOT NULL
                GROUP BY semester
                ORDER BY semester DESC;
            """)
            semester_stats = cur.fetchall()

            cur.execute("""
                SELECT course_name, total_score, letter_grade
                FROM grades
                WHERE total_score IS NOT NULL
                ORDER BY total_score DESC
                LIMIT 5;
            """)
            top_courses = cur.fetchall()

            cur.execute("""
                SELECT letter_grade, COUNT(*) AS count
                FROM grades
                WHERE letter_grade IS NOT NULL
                GROUP BY letter_grade
                ORDER BY letter_grade;
            """)
            grade_distribution = cur.fetchall()

        conn.close()

        return jsonify({
            "semester_stats": semester_stats,
            "top_courses": top_courses,
            "grade_distribution": grade_distribution,
        })

    except Exception as e:
        logger.error(f"API stats error: {e}")
        return jsonify({"error": str(e)}), 500


# ================= INFO =================

@app.route("/info", methods=["GET"])
def info():
    """Service info"""
    return jsonify({
        "service": "Portal UTH Scraper + Dashboard",
        "endpoints": {
            "/": "Dashboard",
            "/grades": "Chi tiết điểm",
            "/scrape": "Trigger scraping",
            "/health": "Health check",
            "/api/grades": "API bảng điểm",
            "/api/stats": "API thống kê",
        }
    }), 200


# ================= RUN =================

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False)
