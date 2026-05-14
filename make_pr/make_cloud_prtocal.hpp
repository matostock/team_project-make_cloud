#pragma once
// ============================================================
//  Protocol.hpp  —  패킷 타입 & JSON 필드 상수 전부 여기서 관리
// ============================================================

#include <cstdint>
#include <string>

// ─────────────────────────────────────────────
//  패킷 헤더 구조체
//  [Header 4byte (Body 길이)] + [Body JSON 문자열]
// ─────────────────────────────────────────────
#pragma pack(push, 1)
struct PacketHeader {
    uint32_t body_length;   // Big-Endian으로 전송
};
#pragma pack(pop)

static constexpr size_t HEADER_SIZE = sizeof(PacketHeader);  // 4 bytes


// ─────────────────────────────────────────────
//  요청 타입 (Client → Server)
// ─────────────────────────────────────────────
namespace REQ {
    // Phase 1 — Auth
    inline constexpr const char* REGISTER        = "REQ_REGISTER";
    inline constexpr const char* LOGIN           = "REQ_LOGIN";

    // Phase 2 — Upload
    inline constexpr const char* UPLOAD_PREFLIGHT = "REQ_UPLOAD_PREFLIGHT";
    inline constexpr const char* UPLOAD_DATA      = "REQ_UPLOAD_DATA";
    inline constexpr const char* UPLOAD_STATUS    = "REQ_UPLOAD_STATUS";

    // Phase 3 — Download & File
    inline constexpr const char* DOWNLOAD        = "REQ_DOWNLOAD";
    inline constexpr const char* FILE_LIST       = "REQ_FILE_LIST";
    inline constexpr const char* FILE_DELETE     = "REQ_FILE_DELETE";

    // Phase 4 — Messenger
    inline constexpr const char* MSG_SEND        = "REQ_MSG_SEND";
    inline constexpr const char* MSG_CHECK       = "REQ_MSG_CHECK";
}


// ─────────────────────────────────────────────
//  응답 타입 (Server → Client)
// ─────────────────────────────────────────────
namespace RES {
    // Phase 1 — Auth
    inline constexpr const char* REGISTER        = "RES_REGISTER";
    inline constexpr const char* LOGIN           = "RES_LOGIN";

    // Phase 2 — Upload
    inline constexpr const char* UPLOAD_PREFLIGHT = "RES_UPLOAD_PREFLIGHT";
    inline constexpr const char* UPLOAD_PENDING   = "RES_UPLOAD_PENDING";
    inline constexpr const char* UPLOAD_STATUS    = "RES_UPLOAD_STATUS";

    // Phase 3 — Download & File
    inline constexpr const char* DOWNLOAD        = "RES_DOWNLOAD";
    inline constexpr const char* FILE_LIST       = "RES_FILE_LIST";
    inline constexpr const char* FILE_DELETE     = "RES_FILE_DELETE";

    // Phase 4 — Messenger
    inline constexpr const char* MSG_SEND        = "RES_MSG_SEND";
    inline constexpr const char* MSG_CHECK       = "RES_MSG_CHECK";

    // 공통 에러
    inline constexpr const char* ERROR           = "RES_ERROR";
}


// ─────────────────────────────────────────────
//  결과 코드
// ─────────────────────────────────────────────
namespace ResultCode {
    inline constexpr const char* OK              = "OK";
    inline constexpr const char* FAIL            = "FAIL";
    inline constexpr const char* QUOTA_EXCEEDED  = "QUOTA_EXCEEDED";
    inline constexpr const char* INVALID_TOKEN   = "INVALID_TOKEN";
    inline constexpr const char* DUPLICATE       = "DUPLICATE";
    inline constexpr const char* NOT_FOUND       = "NOT_FOUND";
    inline constexpr const char* AUTH_FAIL       = "AUTH_FAIL";
}


// ─────────────────────────────────────────────
//  업로드 상태값
// ─────────────────────────────────────────────
namespace UploadStatus {
    inline constexpr const char* PENDING         = "PENDING";
    inline constexpr const char* COMPLETED       = "COMPLETED";
    inline constexpr const char* FAILED          = "FAILED";
}


// ─────────────────────────────────────────────
//  JSON 필드 키 상수 (오타 방지)
// ─────────────────────────────────────────────
namespace Field {
    // 공통
    inline constexpr const char* TYPE            = "type";
    inline constexpr const char* RESULT          = "result";
    inline constexpr const char* MESSAGE         = "message";

    // Auth
    inline constexpr const char* EMAIL           = "email";
    inline constexpr const char* PASSWORD        = "password";
    inline constexpr const char* USER_PK         = "user_pk";
    inline constexpr const char* QUOTA_USED      = "quota_used";
    inline constexpr const char* QUOTA_MAX       = "quota_max";

    // Upload
    inline constexpr const char* TOKEN           = "token";
    inline constexpr const char* FILE_NAME       = "file_name";
    inline constexpr const char* FILE_SIZE       = "file_size";
    inline constexpr const char* FILE_HASH       = "file_hash";   // 클라이언트 측 SHA-256
    inline constexpr const char* STATUS          = "status";

    // Download / File
    inline constexpr const char* FILE_ID         = "file_id";
    inline constexpr const char* FILE_LIST       = "file_list";
    inline constexpr const char* PATH            = "path";

    // Messenger
    inline constexpr const char* SENDER          = "sender";
    inline constexpr const char* RECEIVER        = "receiver";
    inline constexpr const char* CONTENT         = "content";
    inline constexpr const char* MESSAGES        = "messages";
    inline constexpr const char* MSG_ID          = "msg_id";
    inline constexpr const char* SENT_AT         = "sent_at";
}


// ─────────────────────────────────────────────
//  서버 설정 상수
// ─────────────────────────────────────────────
namespace ServerConfig {
    inline constexpr uint16_t    PORT            = 9000;
    inline constexpr int         BACKLOG         = 128;
    inline constexpr size_t      MAX_PACKET_SIZE = 64 * 1024;        // JSON 패킷 최대 64KB
    inline constexpr size_t      MAX_FILE_SIZE   = 1ULL * 1024 * 1024 * 1024; // 파일 최대 1GB
    inline constexpr int         WORKER_THREADS  = 4;
    inline constexpr const char* STORAGE_ROOT    = "./storage";
}