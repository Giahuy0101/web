from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import psycopg2
from psycopg2.extras import execute_values
import os
import time
import logging
from datetime import datetime
from dotenv import load_dotenv

# Load biến môi trường
load_dotenv()

# Setup logging
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)

log_file = os.path.join(log_dir, f"scraper_{datetime.now().strftime('%Y%m%d')}.log")
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file, encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

# Thêm handler cho console
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

# Configuration từ .env
PORTAL_LOGIN_URL = "https://portal.ut.edu.vn"
PORTAL_TRANSCRIPT_URL = "https://portal.ut.edu.vn/transcript"

DB_CONFIG = {
    'host': os.getenv('DB_HOST'),
    'port': int(os.getenv('DB_PORT', 5432)),
    'database': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD')
}

LOGIN_USERNAME = os.getenv('PORTAL_USERNAME')
LOGIN_PASSWORD = os.getenv('PORTAL_PASSWORD')

def create_table(conn):
    """Tạo bảng với unique constraint trên course_code"""
    try:
        with conn.cursor() as cur:
            # Tạo bảng nếu chưa tồn tại
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS grades (
                id SERIAL PRIMARY KEY,
                course_code VARCHAR(50) UNIQUE,
                course_name VARCHAR(255),
                credits INT,
                process_score FLOAT,
                final_score FLOAT,
                total_score FLOAT,
                gpa_4 FLOAT,
                letter_grade VARCHAR(10),
                ranking VARCHAR(50),
                passed BOOLEAN,
                note VARCHAR(255),
                semester VARCHAR(50),
                scraped_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
            cur.execute(create_table_sql)
            
            # Kiểm tra xem constraint đã tồn tại chưa
            check_constraint_sql = """
            SELECT constraint_name 
            FROM information_schema.table_constraints 
            WHERE table_name = 'grades' 
            AND constraint_type = 'UNIQUE'
            AND constraint_name = 'grades_course_code_key';
            """
            cur.execute(check_constraint_sql)
            constraint_exists = cur.fetchone()
            
            if not constraint_exists:
                logger.info("Thêm unique constraint trên course_code...")
                try:
                    alter_table_sql = """
                    ALTER TABLE grades 
                    ADD CONSTRAINT grades_course_code_key 
                    UNIQUE(course_code);
                    """
                    cur.execute(alter_table_sql)
                    logger.info("✓ Đã thêm unique constraint")
                except Exception as e:
                    logger.warning(f"Không thể thêm constraint: {e}")
            else:
                logger.info("✓ Unique constraint đã tồn tại")
            
            conn.commit()
            logger.info("✓ Bảng 'grades' đã sẵn sàng")
    except Exception as e:
        logger.error(f"Lỗi tạo bảng: {e}")
        conn.rollback()
        raise

def login_with_selenium():
    """Đăng nhập bằng Selenium với xử lý reCAPTCHA"""
    
    options = webdriver.ChromeOptions()
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    options.add_argument("--headless=new")  # Headless mode mới
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    
    # Specify Chrome binary location
    options.binary_location = "/usr/bin/google-chrome"
    
    driver = None
    try:
        logger.info("Khởi động Selenium WebDriver...")
        
        # Sử dụng Chrome đã cài trong Docker, không dùng ChromeDriverManager
        service = Service(executable_path='/usr/bin/chromedriver')
        driver = webdriver.Chrome(service=service, options=options)
        
        # Thêm script để tránh detection
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        
        logger.info("Truy cập trang login...")
        driver.get(PORTAL_LOGIN_URL)
        time.sleep(2)
        
        wait = WebDriverWait(driver, 15)
        
        logger.info("Nhập username...")
        username_input = wait.until(
            EC.presence_of_element_located((By.NAME, "username"))
        )
        username_input.clear()
        time.sleep(0.5)
        username_input.send_keys(LOGIN_USERNAME)
        
        logger.info("Nhập password...")
        password_input = driver.find_element(By.NAME, "password")
        password_input.clear()
        time.sleep(0.5)
        password_input.send_keys(LOGIN_PASSWORD)
        
        time.sleep(1)
        
        logger.info("Nhấn nút Đăng nhập...")
        login_button = driver.find_element(By.XPATH, "//button[contains(text(), 'Đăng nhập')]")
        login_button.click()
        
        logger.info("Chờ xử lý đăng nhập...")
        time.sleep(10)
        
        logger.info(f"✓ URL hiện tại: {driver.current_url}")
        
        logger.info("Truy cập trang bảng điểm...")
        driver.get(PORTAL_TRANSCRIPT_URL)
        time.sleep(5)
        
        logger.info("Cào dữ liệu...")
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        
        grades_data = parse_grades(soup)
        logger.info(f"✓ Đã cào {len(grades_data)} môn học")
        
        return grades_data
    
    except Exception as e:
        logger.error(f"Lỗi scraping: {e}")
        import traceback
        traceback.print_exc()
        return None
    
    finally:
        if driver:
            logger.info("Đóng browser...")
            driver.quit()

def parse_grades(soup):
    """Parse dữ liệu bảng điểm - Mỗi course_code chỉ giữ 1 bản ghi mới nhất"""
    grades_data = []
    seen_courses = {}
    
    try:
        tables = soup.find_all('table', {'class': 'MuiTable-root'})
        
        if not tables:
            logger.error("Không tìm thấy bảng dữ liệu!")
            return grades_data
        
        main_table = None
        for table in tables:
            headers = table.find_all('th')
            header_texts = [h.get_text(strip=True) for h in headers]
            if 'Mã lớp học phần' in header_texts:
                main_table = table
                logger.info("✓ Tìm thấy bảng điểm chính")
                break
        
        if not main_table:
            logger.error("Không tìm thấy bảng chính!")
            return grades_data
        
        current_semester = "Unknown"
        tbody = main_table.find('tbody')
        
        if not tbody:
            logger.error("Không tìm thấy tbody!")
            return grades_data
            
        rows = tbody.find_all('tr')
        logger.info(f"Tìm thấy {len(rows)} dòng trong tbody")
        
        for idx, row in enumerate(rows):
            cells = row.find_all('td')
            
            if not cells:
                continue
            
            if len(cells) == 1:
                colspan = cells[0].get('colspan', '0')
                text = cells[0].get_text(strip=True)
                
                if colspan == '12' and (text.startswith('Học kỳ') or text.startswith('học kỳ')):
                    current_semester = text
                    logger.info(f"📚 Phát hiện học kỳ: {current_semester}")
                continue
            
            if cells[0].find('table') or cells[0].find('div', class_='MuiBox-root'):
                continue
            
            if len(cells) != 12:
                continue
            
            try:
                stt = cells[0].get_text(strip=True)
                if not stt.isdigit():
                    continue
                
                course_code = cells[1].get_text(strip=True)
                course_name = cells[2].get_text(strip=True)
                
                if not course_code or not course_code.isdigit() or len(course_code) < 9:
                    continue
                
                if not course_name:
                    continue
                
                process_score_raw = cells[4].get_text(strip=True)
                final_score_raw = cells[5].get_text(strip=True)
                total_score_raw = cells[6].get_text(strip=True)
                
                process_score = safe_float(process_score_raw)
                final_score = safe_float(final_score_raw)
                total_score = safe_float(total_score_raw)
                
                if total_score is None:
                    continue
                
                data = {
                    'course_code': course_code,
                    'course_name': course_name,
                    'credits': safe_int(cells[3].get_text(strip=True)),
                    'process_score': process_score,
                    'final_score': final_score,
                    'total_score': total_score,
                    'gpa_4': safe_float(cells[7].get_text(strip=True)),
                    'letter_grade': cells[8].get_text(strip=True).strip(),
                    'ranking': cells[9].get_text(strip=True),
                    'passed': 'CheckCircleIcon' in str(cells[10]),
                    'note': cells[11].get_text(strip=True) if len(cells) > 11 else '',
                    'semester': current_semester
                }
                
                if course_code in seen_courses:
                    logger.info(f"  🔄 Update: {course_code} - {course_name[:40]}")
                else:
                    logger.info(f"  ✓ {stt}. {course_code} - {course_name[:40]} - Điểm: {total_score}")
                
                seen_courses[course_code] = data
            
            except (ValueError, IndexError) as e:
                logger.warning(f"Lỗi parse dòng {idx}: {e}")
                continue
        
        grades_data = list(seen_courses.values())
        logger.info(f"📊 Tổng số môn unique: {len(grades_data)}")
        return grades_data
    
    except Exception as e:
        logger.error(f"Lỗi parse HTML: {e}")
        import traceback
        traceback.print_exc()
        return grades_data

def safe_float(value):
    """Convert string to float, handling Vietnamese decimal format"""
    try:
        if not value:
            return None
        value = str(value).strip().replace(',', '.')
        return float(value) if value else None
    except:
        return None

def safe_int(value):
    """Convert string to int"""
    try:
        if not value:
            return None
        value = str(value).strip().replace(',', '.')
        return int(float(value)) if value else None
    except:
        return None

def insert_data_to_db(grades_data):
    """Insert dữ liệu với ON CONFLICT handling"""
    if not grades_data:
        logger.error("Không có dữ liệu để insert")
        return
    
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        create_table(conn)
        
        insert_sql = """
        INSERT INTO grades 
        (course_code, course_name, credits, process_score, final_score, 
         total_score, gpa_4, letter_grade, ranking, passed, note, semester)
        VALUES %s
        ON CONFLICT (course_code) 
        DO UPDATE SET
            course_name = EXCLUDED.course_name,
            credits = EXCLUDED.credits,
            process_score = EXCLUDED.process_score,
            final_score = EXCLUDED.final_score,
            total_score = EXCLUDED.total_score,
            gpa_4 = EXCLUDED.gpa_4,
            letter_grade = EXCLUDED.letter_grade,
            ranking = EXCLUDED.ranking,
            passed = EXCLUDED.passed,
            note = EXCLUDED.note,
            semester = EXCLUDED.semester,
            scraped_date = CURRENT_TIMESTAMP
        """
        
        values = [
            (
                g['course_code'], g['course_name'], g['credits'],
                g['process_score'], g['final_score'], g['total_score'],
                g['gpa_4'], g['letter_grade'], g['ranking'],
                g['passed'], g['note'], g['semester']
            )
            for g in grades_data
        ]
        
        with conn.cursor() as cur:
            execute_values(cur, insert_sql, values)
            conn.commit()
            logger.info(f"✓ Đã upsert {len(grades_data)} môn học vào database")
        
        conn.close()
    
    except Exception as e:
        logger.error(f"Lỗi insert database: {e}")
        import traceback
        traceback.print_exc()

def main():
    logger.info("=" * 60)
    logger.info("🚀 BẮT ĐẦU CÀO DỮ LIỆU PORTAL UTH")
    logger.info("=" * 60)
    
    grades_data = login_with_selenium()
    
    if grades_data:
        insert_data_to_db(grades_data)
        logger.info("✅ HOÀN THÀNH!")
    else:
        logger.error("❌ Không thể cào dữ liệu")
    
    logger.info("=" * 60)

if __name__ == "__main__":
    main()
