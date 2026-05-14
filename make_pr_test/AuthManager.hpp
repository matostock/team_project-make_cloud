#pragma once
#include <utility>
#include <iostream>
#include <string>
#include <fstream>
#include <filesystem>
#include <map>
#include <random>
#include <nlohmann/json.hpp>
#include <curl/curl.h>
#include <ctime>
#include <set>
#include <mariadb/mysql.h>

using namespace std;
using namespace std::filesystem;
using json = nlohmann::json;

class AuthManager
{
private:
    path db_file;
    json user_db;
    int next_pk;

    map<string, string>            email_auth_codes;
    map<string, pair<int, time_t>> login_attempts;
    set<string>                    verified_emails;

    MYSQL* db_conn;

    // ── DB 연결 초기화 ────────────────────────────────────────────────────────
    bool initDB()
    {
        db_conn = mysql_init(NULL);
        if (!db_conn)
        {
            cerr << "[AuthDB Error] mysql_init 실패" << endl;
            return false;
        }
        if (!mysql_real_connect(db_conn, "10.10.20.101", "HEECHANG", "1234", "USERS", 0, NULL, 0))
        {
            cerr << "[AuthDB Error] DB 연결 실패: " << mysql_error(db_conn) << endl;
            mysql_close(db_conn);
            db_conn = nullptr;
            return false;
        }
        mysql_set_character_set(db_conn, "utf8mb4");
        cout << "[AuthDB] USERS DB 연결 성공!" << endl;
        return true;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // [핵심] DB MEMBERSHIP 기준으로 이메일 존재 여부 확인
    // DB에서 DELETE하면 false 반환 → 재가입 허용
    // ─────────────────────────────────────────────────────────────────────────
    bool isEmailExistsInDB(const string& id)
    {
        if (!db_conn) return false;

        char safe_id[51];
        mysql_real_escape_string(db_conn, safe_id, id.c_str(), id.size());

        char query[256];
        snprintf(query, sizeof(query),
            "SELECT COUNT(*) FROM MEMBERSHIP WHERE ID = '%s'", safe_id);

        if (mysql_query(db_conn, query))
        {
            cerr << "[AuthDB Error] 이메일 존재 확인 실패: " << mysql_error(db_conn) << endl;
            return false;
        }

        MYSQL_RES* result = mysql_store_result(db_conn);
        if (!result) return false;

        MYSQL_ROW row = mysql_fetch_row(result);
        int count = (row && row[0]) ? atoi(row[0]) : 0;
        mysql_free_result(result);
        return (count > 0);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // [핵심 수정] MEMBERSHIP INSERT → 생성된 실제 USER_NUM 반환
    // 기존: bool 반환(USER_NUM 버림) → 변경: int 반환
    // ─────────────────────────────────────────────────────────────────────────
    int insertMembership(const string& id, const string& pwd_hash, const string& name)
    {
        if (!db_conn)
        {
            cerr << "[AuthDB Error] DB 연결 없음 → INSERT 불가" << endl;
            return -1;
        }

        char safe_id[51];
        char safe_hash[129];
        char safe_name[21]; // NAME varchar(10), 한글 고려 여유

        mysql_real_escape_string(db_conn, safe_id,   id.c_str(),       id.size());
        mysql_real_escape_string(db_conn, safe_hash, pwd_hash.c_str(), pwd_hash.size());
        mysql_real_escape_string(db_conn, safe_name, name.c_str(),     name.size());

        char query[512];
        snprintf(query, sizeof(query),
            "INSERT INTO MEMBERSHIP (ID, PW, NAME) VALUES ('%s', '%s', '%s')",
            safe_id, safe_hash, safe_name);

        if (mysql_query(db_conn, query))
        {
            cerr << "[AuthDB Error] MEMBERSHIP INSERT 실패: " << mysql_error(db_conn) << endl;
            return -1;
        }

        int inserted_num = (int)mysql_insert_id(db_conn);
        cout << "[AuthDB] MEMBERSHIP INSERT 성공! USER_NUM: " << inserted_num << endl;
        return inserted_num;
    }

    // ── DB에서 ID+PW로 실제 USER_NUM 조회 (로그인용) ─────────────────────────
    int queryUserNumFromDB(const string& id, const string& pwd_hash)
    {
        if (!db_conn) return -1;

        char safe_id[51];
        char safe_hash[129];
        mysql_real_escape_string(db_conn, safe_id,   id.c_str(),       id.size());
        mysql_real_escape_string(db_conn, safe_hash, pwd_hash.c_str(), pwd_hash.size());

        char query[512];
        snprintf(query, sizeof(query),
            "SELECT USER_NUM FROM MEMBERSHIP WHERE ID='%s' AND PW='%s'",
            safe_id, safe_hash);

        if (mysql_query(db_conn, query))
        {
            cerr << "[AuthDB Error] USER_NUM 조회 실패: " << mysql_error(db_conn) << endl;
            return -1;
        }

        MYSQL_RES* result = mysql_store_result(db_conn);
        if (!result) return -1;

        MYSQL_ROW row = mysql_fetch_row(result);
        int user_num  = (row && row[0]) ? atoi(row[0]) : -1;
        mysql_free_result(result);
        return user_num;
    }

    // ── libcurl 이메일 전송 ───────────────────────────────────────────────────
    struct WriteThis { const char *readptr; size_t sizeleft; };

    static size_t payload_source(void *ptr, size_t size, size_t nmemb, void *userp)
    {
        WriteThis *upload = (WriteThis *)userp;
        size_t len = size * nmemb;
        if (upload->sizeleft)
        {
            size_t copy = upload->sizeleft;
            if (copy > len) copy = len;
            memcpy(ptr, upload->readptr, copy);
            upload->readptr  += copy;
            upload->sizeleft -= copy;
            return copy;
        }
        return 0;
    }

    bool sendMailViaCurl(const string &target_email, const string &auth_code)
    {
        string my_email     = "taehyunny0312@gmail.com";
        string app_password = "rnwz koev idvf mmna";

        string payload_text =
            "To: "   + target_email + "\r\n" +
            "From: " + my_email     + "\r\n" +
            "Subject: [4erign Cloud] Verification Code\r\n"
            "\r\n"
            "안녕하세요 4(for)eign Cloud 입니다. \n인증번호는: " + auth_code + "\r\n"
            ".\r\n";

        WriteThis upload_data = {payload_text.c_str(), payload_text.size()};
        CURL *curl = curl_easy_init();
        if (!curl) return false;

        curl_easy_setopt(curl, CURLOPT_URL,          "smtps://smtp.gmail.com:465");
        curl_easy_setopt(curl, CURLOPT_USERNAME,     my_email.c_str());
        curl_easy_setopt(curl, CURLOPT_PASSWORD,     app_password.c_str());
        curl_easy_setopt(curl, CURLOPT_MAIL_FROM,    ("<" + my_email + ">").c_str());

        struct curl_slist *recipients =
            curl_slist_append(NULL, ("<" + target_email + ">").c_str());
        curl_easy_setopt(curl, CURLOPT_MAIL_RCPT,    recipients);
        curl_easy_setopt(curl, CURLOPT_READFUNCTION, payload_source);
        curl_easy_setopt(curl, CURLOPT_READDATA,     &upload_data);
        curl_easy_setopt(curl, CURLOPT_UPLOAD,       1L);

        CURLcode res = curl_easy_perform(curl);
        curl_slist_free_all(recipients);
        curl_easy_cleanup(curl);
        return (res == CURLE_OK);
    }

    // ── JSON 로컬 캐시 ────────────────────────────────────────────────────────
    void loadDB()
    {
        ifstream ifs(db_file);
        if (ifs.is_open())
        {
            ifs >> user_db;
            ifs.close();
            next_pk = 1;
            for (auto &[id, info] : user_db.items())
            {
                int current_pk = info["user_pk"];
                if (current_pk >= next_pk) next_pk = current_pk + 1;
            }
        }
        else
        {
            user_db = json::object();
            next_pk = 1;
        }
    }

    void saveDB()
    {
        ofstream ofs(db_file);
        ofs << user_db.dump(4);
        ofs.close();
    }

    string generateAuthCode()
    {
        random_device rd;
        mt19937 gen(rd());
        uniform_int_distribution<> dis(100000, 999999);
        return to_string(dis(gen));
    }

public:
    AuthManager()
    {
        db_file = "users.json";
        loadDB();
        initDB();
    }

    ~AuthManager()
    {
        if (db_conn) { mysql_close(db_conn); db_conn = nullptr; }
    }

    bool verifyEmail(const string &email, const string &input_code)
    {
        if (email_auth_codes.count(email) && email_auth_codes[email] == input_code)
        {
            cout << "[Auth] 이메일 인증 성공: " << email << endl;
            email_auth_codes.erase(email);
            verified_emails.insert(email);
            return true;
        }
        return false;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 이메일 인증 코드 발송 요청
    //
    // [수정] JSON이 아닌 DB 기준으로 중복 확인
    //   DB DELETE → 재가입 가능 / DB에 존재 → 발송 거부
    // ─────────────────────────────────────────────────────────────────────────
    bool requestEmailAuth(const string &email)
    {
        if (isEmailExistsInDB(email))
        {
            cerr << "[Auth] 인증 거부: DB에 이미 가입된 이메일 (" << email << ")" << endl;
            return false;
        }

        string code = generateAuthCode();
        if (sendMailViaCurl(email, code))
        {
            email_auth_codes[email] = code;
            cout << "[Auth] 인증 메일 발송 성공: " << email << endl;
            return true;
        }
        cerr << "[Auth] 인증 메일 발송 실패: " << email << endl;
        return false;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 회원가입
    //
    // [수정]
    //   - 중복 체크: JSON → DB 기준
    //   - 반환값: DB auto_increment USER_NUM (200번~)
    //   - JSON에도 DB USER_NUM으로 동기화
    //
    // 반환값:
    //   양수: 성공 (DB USER_NUM)   -1: 이미 존재   -2: DB오류   -3: 인증미완료
    // ─────────────────────────────────────────────────────────────────────────
    int registerUser(const string &id, const string &pwd_hash, const string &name = "")
    {
        if (verified_emails.find(id) == verified_emails.end())
        {
            cerr << "[Auth] 가입 실패: 인증되지 않은 이메일" << endl;
            return -3;
        }

        if (isEmailExistsInDB(id))
        {
            cerr << "[Auth] 가입 실패: DB에 이미 존재하는 이메일 (" << id << ")" << endl;
            verified_emails.erase(id);
            return -1;
        }

        int new_db_num = insertMembership(id, pwd_hash, name);
        if (new_db_num < 0)
        {
            cerr << "[Auth] 가입 실패: DB INSERT 오류" << endl;
            verified_emails.erase(id);
            return -2;
        }

        // JSON에 DB USER_NUM으로 동기화 저장
        user_db[id] = {{"user_pk", new_db_num}, {"pwd_hash", pwd_hash}};
        saveDB();
        verified_emails.erase(id);

        cout << "[Auth] 회원가입 성공: " << id << " | USER_NUM: " << new_db_num << endl;
        return new_db_num;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 로그인
    //
    // [수정] DB에서 직접 USER_NUM 조회 반환
    //   → FILE_PATH FK 위반 해결
    //
    // 반환값:
    //   양수: 성공 (DB USER_NUM)   -1: 불일치   -2: 잠금
    // ─────────────────────────────────────────────────────────────────────────
    int loginUser(const string &id, const string &pwd_hash)
    {
        time_t now = time(nullptr);

        if (login_attempts.count(id))
        {
            if (now < login_attempts[id].second)
            {
                long remain = login_attempts[id].second - now;
                cerr << "[Auth] 잠금 상태 (" << remain << "초 남음)" << endl;
                return -2;
            }
            else if (login_attempts[id].second != 0)
                login_attempts[id] = {0, 0};
        }

        int real_user_num = queryUserNumFromDB(id, pwd_hash);

        if (real_user_num > 0)
        {
            login_attempts[id] = {0, 0};
            cout << "[Auth] 로그인 성공! ID: " << id
                 << " | USER_NUM: " << real_user_num << endl;
            return real_user_num;
        }
        else
        {
            login_attempts[id].first++;
            int fails = login_attempts[id].first;
            cerr << "[Auth] 로그인 실패: " << fails << "회 틀림" << endl;

            if (fails >= 5)
            {
                login_attempts[id].second = now + 180;
                cerr << "[Auth] 5회 오류 → 3분 계정 잠금!" << endl;
                return -2;
            }
            return -1;
        }
    }
};
