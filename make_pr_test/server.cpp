#include <iostream>            // 표준 입출력 스트림(cout, cerr)을 위한 헤더
#include <vector>              // 클라이언트 소켓들을 관리할 벡터 컨테이너 헤더
#include <sys/socket.h>        // 소켓 생성 및 통신 관련 시스템 호출 헤더
#include <netinet/in.h>        // 인터넷 프로토콜(IP, Port) 구조체 관련 헤더
#include <unistd.h>            // POSIX 시스템 호출(close 등)을 위한 헤더
#include <thread>              // 멀티스레딩 구현을 위한 헤더
#include <algorithm>           // 벡터 요소 조작(remove 등)을 위한 헤더
#include <mutex>               // 공유 자원 보호를 위한 상호 배제(Lock) 헤더
#include <sstream>             // 문자열 스트림 작업을 위한 헤더
#include <iomanip>             // 입출력 형식 지정(16진수 변환 등) 헤더
#include <cstring>             // 메모리 초기화(memset) 등을 위한 헤더
#include <string>
#include <memory>              // 스마트 포인터(unique_ptr) 사용을 위한 헤더
#include <nlohmann/json.hpp>   // JSON 파싱 및 생성을 위한 라이브러리 헤더
#include <openssl/evp.h>       // OpenSSL의 암호화 알고리즘(EVP interface) 헤더
#include <mariadb/mysql.h>     // MariaDB(MySQL) 클라이언트 API 헤더
#include <arpa/inet.h>
#include <queue>
#include <condition_variable>
#include "make_cloud_protocol.hpp"
#include "Protocol.hpp"
#include "StorageManager.hpp"
#include "AuthManager.hpp"
#include "MsgServerLogic.hpp"   // 메시지 서버 (포트 9001)
#include "UserManager.hpp"
#include "AdminManager.hpp"

#define OPENSSL_API_COMPAT 0x30000000L

using namespace std;           // 표준 네임스페이스 사용
using namespace std::filesystem;
using json = nlohmann::json;   // nlohmann::json을 json으로 축약 사용

// ═══════════════════════════════════════════════════════════
//  [전역 자원] 멀티스레드 환경을 위한 공유 자원
// ═══════════════════════════════════════════════════════════
vector<int> client_sockets; // 현재 접속 중인 클라이언트 소켓 목록
mutex v_mtx;                // 벡터 접근 보호용 뮤텍스
int MAX_USERS = 30;         // 서버 최대 수용 인원 (관리자가 변경 가능하도록 변수화)

//  HisockServer 클래스: 소켓 서버 및 DB 연동 담당
class HisockServer 
{
    private:

    // std::vector<int>  client_sockets; // 접속한 클라이언트 소켓들의 목록
    MYSQL* conn;                                // MariaDB 연결 핸들러
    AuthManager auth;
    StorageManager storage;
    UserManager user_mgr;
    AdminManager admin;

    std::vector<std::thread> worker_threads;    // 워커 스레드 보관함
    std::queue<int> job_queue;                  // 클라이언트 소켓 대기열
    std::condition_variable cv;                 // 스레드 깨우기용 신호기

    std::mutex socket_mutex;                // 멀티스레드 환경에서 소켓 목록 보호를 위한 뮤텍스
    std::mutex job_mutex;                   // 작업 큐 배정 위한 뮤텍스
    std::mutex select_mutex;                // master_fds 보호용 뮤텍스

    int server_fd;                          // 서버 리스닝 소켓 디스크립터
    fd_set master_fds;                      // 전체 감시 소켓 셋
    fd_set master_fds_copy;                 // select용 복사본
    int fd_max;                             // 최대 소켓 번호
    int fd_max_copy;                        // select용 복사본
    bool stop_server = false;               // 서버 종료 플래그

    struct ClientSession    // 클라이언트 진행 상태
    {
        bool is_uploading = false;
        int current_user_pk = -1;
        int current_file_pk = -1;
        std::string current_original_name = "";
        size_t current_file_size = 0;
        std::string session_email = "";
        char client_ip[INET_ADDRSTRLEN];
    };

    std::map<int, ClientSession> client_sessions; // 소켓별 세션 맵
    std::mutex session_mutex; // 세션 맵 보호용 뮤텍스
        
    public:
    // 생성자: 포트 번호를 인자로 받음
    HisockServer(int port) : server_fd(-1), conn(nullptr), stop_server(false), user_mgr(auth, storage), admin(auth, storage)
    { 
        // 1. MariaDB 초기화 및 접속
        conn = mysql_init(NULL); // MariaDB 연결 객체 초기화

        if (mysql_real_connect(conn, "10.10.20.101", "HEECHANG", "1234", "USERS", 3306, NULL, 0))
                                    // 앞 4개 요소 : 서버주소, 아이디, 비밀번호로 DB 접속 시도
                                    // 뒤 4개 요소 : NULL을 넣으면 특정 DB 선택 없이 접속만 수행
            cout << "MariaDB 접속 성공! (데이터베이스 미지정 상태)" << endl; // 접속 성공 메시지 출력
        else // 접속 실패 시
        { 
            cerr << "MariaDB 접속 실패: " << mysql_error(conn) << endl; // 에러 원인 출력
            exit(1); // 프로그램 종료
        }

        mysql_set_character_set(conn, "utf8mb4"); // 한글 처리를 위한 캐릭터셋 설정
        
        // 2. 서버 리스닝 소켓 설정
        server_fd = socket(AF_INET, SOCK_STREAM, 0); // IPv4, TCP 방식의 소켓 생성
        int opt = 1; // 소켓 옵션 값
        setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)); // 서버 재시작 시 포트 바인딩 에러 방지 설정

        struct sockaddr_in address{}; // 서버 주소 구조체 초기화
        address.sin_family      = AF_INET; // IPv4 주소 체계 설정
        address.sin_addr.s_addr = INADDR_ANY; // 서버의 모든 인터페이스(IP)로부터 접속 허용
        address.sin_port        = htons(port); // 포트 번호를 네트워크 바이트 순서로 변환하여 저장

        // 소켓에 주소와 포트 할당
        if (bind(server_fd, (struct sockaddr*)&address, sizeof(address)) < 0)
        {
            perror("Bind 에러"); // 실패 시 시스템 에러 출력
            exit(1); // 프로그램 종료
        }
        listen(server_fd, 10); // 클라이언트 대기열 설정 (최대 10명까지 대기)
        cout << "[Server] 클라이언트 접속 대기 중... (Port: " << port << ")" << endl;


        // 3. 워커 스레드 풀 생성
        int thread_count = 4; // 워커 스레드 4개 생성

        for (int i = 0; i < thread_count; ++i)
        {
            worker_threads.emplace_back([this, i]() 
            {
                cout << "워커 스레드 #" << i << " 가동 시작" << endl;                
                while (true) 
                {
                    int client_socket = -1;
                    {
                        std::unique_lock<std::mutex> lock(this->job_mutex); // 뮤텍스를 이용해 작업 큐에 안전하게 접근                            
                        this->cv.wait(lock, [this] {return !this->job_queue.empty() || stop_server;}); // 일감이 올 때까지 스레드는 대기(Wait).

                        if (stop_server && job_queue.empty()) return; // 서버 종료 시 스레드 탈출
                        
                        client_socket = job_queue.front(); // 큐에서 가장 오래 기다린 소켓을 하나 꺼낸다.
                        job_queue.pop();
                    } // 여기서 mutex_lock 해제

                    // 꺼낸 소켓을 가지고 실제 작업.
                    if (client_socket != -1)
                        this->handle_client(client_socket);
                }
            });
        }

        storage.initStorage();
        cout << "[Server] 모든 매니저(Auth, Storage, User, Admin) 초기화 완료." << endl;
        cout << "[Server] 저장소 준비 완료" << endl;
        cout << "서버가 " << port << " 포트에서 대기중입니다." << endl; // 서버 시작 알림

        // 메시지 서버 별도 스레드로 실행 (포트 9001)
        thread([]{ serverMain(); }).detach();
        cout << "[Server] 메시지 서버 스레드 시작 (Port: 9001)" << endl;
    }

    // 소멸자: 프로그램 종료 시 자원 해제
    ~HisockServer() 
    {
        {
            std::lock_guard<std::mutex> lock(socket_mutex);
            stop_server = true;
        }

        cv.notify_all(); // 모든 워커 스레드 깨우기
        for (auto& t : worker_threads)
            if (t.joinable()) t.join(); // 스레드 종료 대기

        if (conn) mysql_close(conn); // DB 연결 닫기
        if (server_fd != -1) close(server_fd); // 서버 리스닝 소켓 닫기
    }
    
    // select를 적용한 실행 함수    
    void run()
    {
        FD_ZERO(&master_fds);
        FD_SET(server_fd, &master_fds); // 서버 리스닝 소켓 등록
        fd_max = server_fd;

        while (!stop_server)
        {
            // select를 호출하기 전 복사할 때 뮤텍스 보호
            {
                std::lock_guard<std::mutex> lock(select_mutex);
                master_fds_copy = master_fds; // select는 실행 후 원본을 수정하므로 복사본 사용
                fd_max_copy = fd_max;
            }

            // timeout을 설정하여 주기적으로 확인
            struct timeval timeout;
            timeout.tv_sec = 3;         // 3초 정도?
            timeout.tv_usec = 0;

            // 활동이 있는 소켓이 생길 때까지 대기 (Blocking)
            if (select(fd_max_copy + 1, &master_fds_copy, NULL, NULL, &timeout) == -1) break;

            for (int i = 0; i <= fd_max; i++)
            {
                if (FD_ISSET(i, &master_fds_copy))
                {
                    if (i == server_fd) // i번 소켓에 신호 감지
                    {
                        // 신규 접속 처리
                        struct sockaddr_in client_addr{};
                        socklen_t addr_len = sizeof(client_addr);
                        int new_socket = accept(server_fd, (struct sockaddr*)&client_addr, &addr_len);
                        
                        if (new_socket != -1)
                        {
                            // 접속자 IP 추출
                            char client_ip[INET_ADDRSTRLEN];
                            inet_ntop(AF_INET, &client_addr.sin_addr, client_ip, INET_ADDRSTRLEN);

                        // ─────────────────────────────────────────────────────────
                        // [방어 1단계] IP 블랙리스트 체크 (문지기)
                        // ─────────────────────────────────────────────────────────
                        if (admin.isAccessDenied(client_ip))
                        {
                            cout << "[Block] 차단된 IP 접속 시도 거부: " << client_ip << endl;
                            close(new_socket); // 아예 상대해주지 않음
                            continue;
                        }

                        // ─────────────────────────────────────────────────────────
                        // [안내 2단계] 핸드셰이크 헤더 발송 (안내데스크)
                        // ─────────────────────────────────────────────────────────
                        ServerHandshakeHeader header = {0};
                        header.type = PKT_RES_HANDSHAKE;
                        header.server_version = 1.2f; // 서버 버전 1.2로 설정

                        v_mtx.lock();
                        header.current_users = client_sockets.size();
                        header.max_users = MAX_USERS;
                        v_mtx.unlock();
                        header.next_port = 0; // 나중에 변경 예약 시 사용

                        // 무조건 명함(헤더)부터 건넴
                        send(new_socket, (char *)&header, sizeof(header), 0);


                        // 정원 초과 시 연결 종료 (헤더는 보냈으니 클라이언트가 알고 끊음)
                        if (header.current_users >= header.max_users)
                        {
                            cout << "[Full] 정원 초과로 접속 거부: " << client_ip << endl;
                            close(new_socket);
                            continue;
                        }

                            std::lock_guard<std::mutex> lock(select_mutex);
                            FD_SET(new_socket, &master_fds); // 감시 대상에 추가
                            if (new_socket > fd_max) fd_max = new_socket;
                            cout << "[Server] 새 연결 추가: " << new_socket << endl;
                        }
                    } 
                    else    // 데이터 도착: 감시 대상에서 일시 제외 (워커 스레드 점유 시작)
                    {
                        std::lock_guard<std::mutex> lock(select_mutex);
                        FD_CLR(i, &master_fds);
                        {
                            std::lock_guard<std::mutex> lock(job_mutex);
                            job_queue.push(i); // 워커 스레드에게 소켓 번호 전달
                        }
                        cv.notify_one();
                    }
                }
            }
        }
    }

    void server_response(int client_socket, const json& res_json) // 공통 답변 함수(서버 -> 클라이언트)
    { 
        std::string body = res_json.dump();
        PacketHeader header;
        header.body_length = htonl(static_cast<uint32_t>(body.size()));

        send(client_socket, &header, sizeof(header), 0);
        send(client_socket, body.c_str(), body.size(), 0);
    }

    int recv_all(int sock, char *buf, int size)
    {
        int total_recv = 0;
        while (total_recv < size)
        {
            int len = recv(sock, buf + total_recv, size - total_recv, 0);
            if (len <= 0)
                return len;
            total_recv += len;
        }
        return total_recv;
    }

    // 클라이언트 접속 종료 시 목록에서 안전하게 제거
    void remove_client(int sock)
    {
        lock_guard<mutex> lock(v_mtx);
        client_sockets.erase(remove(client_sockets.begin(), client_sockets.end(), sock), client_sockets.end());
        close(sock);
    }

    void handle_client(int client_socket)
    // (int client_socket, string client_ip,
    //                 AuthManager &auth, StorageManager &storage,
    //                 UserManager &user_mgr, AdminManager &admin) // 개별 클라이언트와의 통신 처리 함수
    {
        // 0. 소켓에 해당하는 상태값 가져옴
        ClientSession* session;

        {
            std::lock_guard<std::mutex> lock(session_mutex);
            session = &client_sessions[client_socket];
        }

        // 1. 패킷 타입 확인 (Peek)
        int16_t peek_type;
        ssize_t n = recv(client_socket, (char *)&peek_type, sizeof(int16_t), MSG_PEEK);
        if (n <= 0)
        {
            std::lock_guard<std::mutex> lock(session_mutex);
            client_sessions.erase(client_socket);
            cout << "클라이언트 종료 또는 에러 : " << session->client_ip << endl;
            // close(client_socket);
            remove_client(client_socket); // 접속자 명단에서 제거
            return;
        }

        FilePacket struct_packet;   // 페킷 처리용 임시 버퍼(스택 할당으로 누수 방지)

        // 2. 조건문에 의한 분기 (일단 100 이상은 JSON, 이하는 구조체)
        if (peek_type >= 100) // JSON
        {
            PacketHeader header;
            if(recv(client_socket, &header, sizeof(header), 0) > 0) // 실제 헤더 수신
            {
                uint32_t body_len = ntohl(header.body_length); // 엔디언 변환 (요구사항 16 유연성: 네트워크 바이트 오더 -> 호스트 바이트 오더)
                
                std::vector<char> buffer(body_len);
                recv(client_socket, buffer.data(), body_len, MSG_WAITALL);

                try
                {
                    auto j = json::parse(std::string(buffer.begin(), buffer.end()));

                    // 프로토콜 분석 및 분기
                    std::string type = j[Field::TYPE];
                    std::cout << "요청사항 : " << type << std::endl;

                    // 여기에 json 작업 배치
                }
                
                catch (const std::exception& e)
                {
                    // cerr << "JSON 파싱 에러: " << e.what() << endl;
                    cout << "[Error] 스레드 예외 발생: " << e.what() << endl;
                }
            }
        }
        else    // 구조체
        {
            int16_t packet_type = peek_type;
            int target_size; // 수신할 데이터의 크기

            if(packet_type == PKT_REQ_LOGIN || packet_type == PKT_REQ_REGISTER)          // 회원가입/로그인은 인증 관련 패킷
                target_size = sizeof(AuthPacket); 
            else                                                // 그 외에는 파일 관련 패킷으로 간주
                target_size = sizeof(FilePacket);

            if(recv_all(client_socket, (char *)&struct_packet, target_size) > 0)
            {
                int fake_db_pk_counter = 100;

                switch (static_cast<PacketType>(struct_packet.type))
                {
                    // ── [파일 업로드] ──────────────────────────────────────────
                    case PKT_REQ_UPLOAD_START:
                    {
                        cout << "[Upload] 요청 - 유저PK: " << struct_packet.user_pk << endl;

                        session->current_original_name = struct_packet.data;
                        session->current_file_size = struct_packet.file_size;

                        // [변경] 하드코딩된 용량 대신 UserManager를 통해 '등급별 용량' 조회
                        size_t max_quota = user_mgr.getMaxQuota(session->current_user_pk);
                        size_t current_used = storage.getUserTotalUsed(session->current_user_pk);

                        if (current_used + session->current_file_size > max_quota)
                        {
                            cout << "[거부] 용량 초과 (" << (max_quota / 1024 / 1024) << "MB 제한)" << endl;
                            FilePacket res = {};
                            res.type = PKT_RES_UPLOAD_END;
                            res.file_pk = -1; // 실패 신호
                            send(client_socket, (char *)&res, sizeof(FilePacket), 0);
                            break;
                        }

                        storage.createUserDirectory(struct_packet.user_pk);
                        fake_db_pk_counter++;

                        //=이 부분 필요한지 확인 ===
                        // session->is_uploading = true;
                        session->current_user_pk = struct_packet.user_pk;
                        session->current_file_pk = fake_db_pk_counter;
                        //======================

                        FilePacket res = {};
                        res.type = PKT_RES_UPLOAD_START;
                        res.file_pk = fake_db_pk_counter; // 실제론 DB insert 후 PK 반환
                        // res.file_pk = session->current_file_pk;
                        send(client_socket, (char *)&res, sizeof(FilePacket), 0);
                        break;
                    }
                    case PKT_REQ_UPLOAD_CHUNK:
                    {
                        storage.saveTempChunk(session->current_user_pk, session->current_file_pk, struct_packet.data, struct_packet.data_size);
                        break;
                    }
                    case PKT_REQ_UPLOAD_END:
                    {
                        int db_file_pk = -1;
                        // 1. 임시 파일을 최종 위치로 이동 (물리적 처리)
                        if (storage.moveFileToFinal(session->current_user_pk, session->current_file_pk,
                                                    session->current_original_name, session->current_file_size, &db_file_pk))
                        {
                            // 2. 최종 저장된 서버 경로 생성 (예: ./storage/server/101/105.dat)
                            // StorageManager 내부 로직에 따른 경로 형식을 넣어주세요.
                            std::string server_path = "./storage/server/" + std::to_string(struct_packet.user_pk) + "/" + std::to_string(struct_packet.file_pk) + ".dat";

                            // 3. [핵심] DB에 기록 (태현님의 테이블 명세 반영)
                            storage.recordFileToDB(struct_packet.user_pk, session->current_original_name, server_path, session->current_file_size);


                            cout << "[Upload End] 파일 저장 및 DB 등록 완료: " << session->current_original_name << endl;

                            // cout << "[Server] 업로드 확정 완료. 파일: " << session->current_original_name
                            //      << " | DB FILE_PK: " << db_file_pk << endl;
                            // session->is_uploading = false;

                            FilePacket res = {};
                            res.type = PKT_RES_UPLOAD_END;
                            res.file_pk = struct_packet.file_pk;
                            send(client_socket, (char *)&res, sizeof(FilePacket), 0);
                        }
                        break;
                    }
                    // ── [파일 다운로드] ────────────────────────────────────────
                    case PKT_REQ_DOWNLOAD_START:
                    {
                        // 다운로드 로직은 기존 바이너리 방식을 유지하되, 필요 시 JSON 리스트에서 정보를 찾도록 확장 가능
                        size_t fsize = storage.getFileSize(struct_packet.user_pk, struct_packet.file_pk);
                        FilePacket res = {};
                        res.type = PKT_RES_DOWNLOAD_START;
                        res.file_size = fsize;
                        send(client_socket, (char *)&res, sizeof(FilePacket), 0);

                        if (fsize > 0)
                        {
                            size_t offset = 0;
                            while (offset < fsize)
                            {
                                FilePacket chunk = {};
                                chunk.type = PKT_RES_DOWNLOAD_DATA;
                                size_t read_bytes = storage.readFileChunk(struct_packet.user_pk, struct_packet.file_pk, offset, chunk.data);
                                if (read_bytes > 0)
                                {
                                    chunk.data_size = (int)read_bytes;
                                    if (send(client_socket, (char *)&chunk, sizeof(FilePacket), 0) <= 0)
                                        break;
                                    offset += read_bytes;
                                }
                                else
                                    break;
                            }
                        }
                        break;
                    }
                    // ── [이메일 인증] ──────────────────────────────────────────
                    case PKT_REQ_EMAIL_AUTH:
                    {
                        session->session_email = struct_packet.data;
                        
                        FilePacket res = {};
                        res.type = PKT_RES_EMAIL_AUTH;
                        res.file_pk = auth.requestEmailAuth(session->session_email) ? 1 : -1;
                        send(client_socket, (char *)&res, sizeof(FilePacket), 0);
                    
                        break;

                        // session->session_email = struct_packet.data; // 클라이언트가 보낸 이메일을 세션에 저장
                        // cout << "[Server] 인증 번호 요청 접수: " << session->session_email << endl;

                        // FilePacket res = {};
                        // res.type = PKT_RES_EMAIL_AUTH;

                        // if (auth.requestEmailAuth(session->session_email))
                        // {
                        //     res.file_pk = 1; // 성공 시 1
                        //     cout << "[Server] 메일 발송 성공" << endl;
                        // }
                        // else
                        // {
                        //     res.file_pk = -1; // 실패 시 -1
                        //     cout << "[Server] 메일 발송 실패" << endl;
                        // }
                        // send(client_socket, (char *)&res, sizeof(FilePacket), 0);
                        // break;
                    }
                    case PKT_REQ_EMAIL_VERIFY:
                    {
                        string code = struct_packet.data;
                        FilePacket res = {};
                        res.type = PKT_RES_EMAIL_VERIFY;
                        res.file_pk = auth.verifyEmail(session->session_email, code) ? 1 : -1;
                        send(client_socket, (char *)&res, sizeof(FilePacket), 0);

                        break;
                        

                        // string input_code = struct_packet.data; // 유저가 입력한 6자리 번호
                        // cout << "[Server] 인증 번호 검증 시도: " << session->session_email << " -> " << input_code << endl;

                        // FilePacket res = {};
                        // res.type = PKT_RES_EMAIL_VERIFY;

                        // if (auth.verifyEmail(session->session_email, input_code))
                        // {
                        //     res.file_pk = 1; // 인증 성공
                        //     // cout << "[Server] 인증 성공!" << endl;
                        // }
                        // else
                        // {
                        //     res.file_pk = -1; // 인증 실패
                        //     // cout << "[Server] 인증 실패 (번호 불일치)" << endl;
                        // }
                        // send(client_socket, (char *)&res, sizeof(FilePacket), 0);
                        // break;
                    }
                    // ── [회원가입] ─────────────────────────────────────────────
                    case PKT_REQ_REGISTER:
                    {
                        AuthPacket *auth_pkt = (AuthPacket *)&struct_packet;
                        int new_pk = auth.registerUser(auth_pkt->id, auth_pkt->pwd_hash, auth_pkt->name);

                        if (new_pk > 0)
                        {                           
                            if (user_mgr.initializeUserSettings(new_pk))
                            {
                                cout << "[Register] 초기 환경 설정 완료: PK " << new_pk << endl;
                            }
                        }

                        AuthResponse res = {};
                        res.type = PKT_RES_REGISTER;
                        res.user_pk = new_pk;
                        send(client_socket, (char *)&res, sizeof(AuthResponse), 0);
                        break;
                    }
                    // ── [로그인] ───────────────────────────────────────────────
                    case PKT_REQ_LOGIN:
                    {
                        AuthPacket *auth_pkt = (AuthPacket *)&struct_packet;
                        int login_pk = auth.loginUser(auth_pkt->id, auth_pkt->pwd_hash);

                        // [추가] 로그인 성공 시, 혹시 차단된 계정인지 2차 확인 (AdminManager)
                        if (login_pk > 0 && admin.isAccessDenied(session->client_ip, login_pk))
                        {
                            cout << "[Security] 차단된 계정 접속 시도: " << login_pk << endl;
                            login_pk = -1; // 로그인 실패 처리
                        }

                        AuthResponse res = {};
                        res.type = PKT_RES_LOGIN;
                        res.user_pk = login_pk;
                        send(client_socket, (char *)&res, sizeof(AuthResponse), 0);
                        break;
                    }
                    default:
                        break;
                }
            }
        }

        // 3. 작업 완료 후 다시 select 감시 대상으로 복구
        std::lock_guard<std::mutex> lock(select_mutex);
        FD_SET(client_socket, &master_fds);
    }   // 함수가 종료되면서 워커 스레드는 다음 job을 수행할 준비
};

int main()
{
    HisockServer server(9000); // 9000 포트를 사용하는 서버 객체 생성
    server.run(); // 서버 구동 시작
    return 0; // 프로그램 종료
}