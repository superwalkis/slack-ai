const Anthropic = require('@anthropic-ai/sdk');
const { WebClient } = require('@slack/web-api');
const { Client } = require('@notionhq/client');
const { google } = require('googleapis');

// ============================================
// 클라이언트 초기화
// ============================================
const anthropic = new Anthropic({
  apiKey: process.env.ANTHROPIC_API_KEY,
});

const slack = new WebClient(process.env.SLACK_BOT_TOKEN);
const slackUser = new WebClient(process.env.SLACK_USER_TOKEN);

const notion = new Client({
  auth: process.env.NOTION_API_KEY,
});

// ============================================
// ✅ 수정 1: 날짜 유효성 검사 함수 추가
// ============================================
function isValidDateRow(dateStr) {
  if (!dateStr) return false;
  const value = String(dateStr).trim();
  // "현재까지", 빈 값, 누적 등 제외
  if (value === '' || value.includes('현재까지') || value.includes('누적')) return false;
  // YYYY-MM-DD 형식만 허용
  return /^\d{4}-\d{2}-\d{2}$/.test(value);
}

// ============================================
// ✅ 수정 2: formatWon 함수 개선 (억/만원 단위 명확화)
// ============================================
function formatWon(amount) {
  if (!amount || amount === 0) return '₩0';

  // 1억 이상이면 "xx.x억"
  if (amount >= 100_000_000) {
    const v = (amount / 100_000_000).toFixed(1);
    return `₩${v}억`;
  }

  // 100만 이상이면 "xxx.x만" 
  if (amount >= 1_000_000) {
    const v = (amount / 10_000).toFixed(1);
    return `₩${v}만`;
  }

  // 1만 이상이면 "xx.x만"
  if (amount >= 10_000) {
    const v = (amount / 10_000).toFixed(1);
    return `₩${v}만`;
  }

  return '₩' + amount.toLocaleString('ko-KR');
}

// ============================================
// Google Sheets 매출 데이터 수집
// ============================================
async function getRevenueData(days = 7) {
  try {
    const credentials = JSON.parse(process.env.GOOGLE_SERVICE_ACCOUNT_JSON || '{}');
    
    if (!credentials.client_email) {
      console.log('Google 서비스 계정 미설정 - 매출 데이터 스킵');
      return null;
    }

    const auth = new google.auth.GoogleAuth({
      credentials,
      scopes: ['https://www.googleapis.com/auth/spreadsheets.readonly'],
    });

    const sheets = google.sheets({ version: 'v4', auth });
    
    const spreadsheetId = '1e97jBZ9tSsJ0RiU8aGwp_t6w5RW-5olZ8G1fLYhTy8g';
    
    // 현재 월 시트 이름 (25.12 형식)
    const now = new Date();
    const sheetName = `${String(now.getFullYear()).slice(2)}.${String(now.getMonth() + 1).padStart(2, '0')}`;
    
    console.log(`📊 시트 이름: ${sheetName}`);
    
    // A열부터 AB열까지 전체 가져오기
    const range = `${sheetName}!A:AB`;
    
    const response = await sheets.spreadsheets.values.get({
      spreadsheetId,
      range,
    });

    const rows = response.data.values;
    if (!rows || rows.length < 4) {
      console.log('매출 데이터 없음 - 행 수:', rows?.length || 0);
      return null;
    }

    console.log(`📊 가져온 행 수: ${rows.length}`);
    console.log(`📊 2행(헤더): ${rows[1]?.slice(0, 5).join(', ')}...`);

    // 실제 구조:
    // 1행: 대분류 헤더
    // 2행: 세부 헤더 (날짜/소분류, 래플 응모, 팀워크...)
    // 3행: 누적 합계 ← 이 행은 제외해야 함!
    // 4행~: 일별 데이터
    // 마지막 컬럼(27): GRND 종가 ← 매출이 아님, 제외!
    
    // 헤더 가져오기
    const headers = rows[1];

    // 주요 컬럼 인덱스 찾기
    const findCol = (keywords) => {
      return headers.findIndex(h => h && keywords.some(k => h.includes(k)));
    };

    // ✅ 수정: 모든 수익 컬럼 찾기 (GRND 종가, 날짜 제외)
    const revenueColIndexes = [];
    const excludeKeywords = ['날짜', 'GRND', '종가', '소분류'];
    
    headers.forEach((header, idx) => {
      if (!header) return;
      const isExcluded = excludeKeywords.some(k => header.includes(k));
      if (!isExcluded && idx > 0) {
        revenueColIndexes.push(idx);
      }
    });
    
    console.log(`📊 수익 컬럼 인덱스들: ${revenueColIndexes.join(', ')} (총 ${revenueColIndexes.length}개)`);

    const COL = {
      날짜: 0,
      래플응모: findCol(['래플 응모', '래플응모']),
      팀워크: findCol(['팀워크']),
      스팀팩상자개봉: findCol(['스팀팩 상자 개봉', '스팀팩']),
      신발소켓개방: findCol(['신발 소켓 개방', '신발 소켓']),
      장비소켓개방: findCol(['장비 소켓 개방', '장비 소켓']),
      첫구매패키지: findCol(['첫구매 패키지', '첫구매']),
      슈퍼즈응원단슬롯개방: findCol(['슈퍼즈 응원단 슬롯', '응원단 슬롯']),
      슈퍼즈캔디구매: findCol(['슈퍼즈 캔디', '캔디 구매']),
      확률구매신발: findCol(['확률 구매(신발)', '확률구매(신발)']),
      확률구매슈퍼즈: findCol(['확률 구매(슈퍼즈)', '확률구매(슈퍼즈)']),
      거래수수료신발: findCol(['거래수수료(신발)']),
      거래수수료슈퍼즈: findCol(['거래수수료(슈퍼즈)']),
      이벤트상점: findCol(['이벤트 상점', '이벤트상점']),
      특가상품: findCol(['특가 상품', '특가상품', '특가']),
      자동수리패스: findCol(['자동수리패스', '자동수리']),
      자동멈춤패스: findCol(['자동멈춤패스', '자동멈춤']),
      옵션보관함A: findCol(['옵션보관함(A)']),
      옵션보관함B: findCol(['옵션보관함(B)']),
      옵션보관함C: findCol(['옵션보관함(C)']),
      교환수수료: findCol(['교환 수수료', '교환수수료']),
      네트워크: findCol(['네트워크']),
      직판: findCol(['직판']),
      공략집: findCol(['공략집']),
      배경화면: findCol(['배경화면']),
      수익컬럼들: revenueColIndexes,
    };
    
    console.log(`📊 컬럼 매핑: 이벤트상점=${COL.이벤트상점}, 거래수수료신발=${COL.거래수수료신발}, 네트워크=${COL.네트워크}, 특가=${COL.특가상품}`);

    console.log(`📊 컬럼 매핑: 날짜=${COL.날짜}, 특가=${COL.특가상품}, 광고네트워크=${COL.광고네트워크}`);

    // ✅ 디버깅: 헤더 전체 출력
    console.log(`📊 전체 헤더(2행): ${headers.join(' | ')}`);
    
    // ✅ 디버깅: 처음 5개 데이터 행 원본 출력
    console.log(`📊 === 원본 데이터 샘플 (4~8행) ===`);
    for (let i = 3; i < Math.min(8, rows.length); i++) {
      const row = rows[i];
      // 모든 수익 컬럼 합산
      let rowTotal = 0;
      for (const colIdx of COL.수익컬럼들) {
        rowTotal += parseNumber(row[colIdx]);
      }
      console.log(`  행${i+1}: A="${row[0]}" | 특가(${COL.특가상품})="${row[COL.특가상품]}" | 광고네트워크(${COL.광고네트워크})="${row[COL.광고네트워크]}" | 전체합산=${formatWon(rowTotal)}`);
    }
    console.log(`📊 === 원본 데이터 샘플 끝 ===`);

    // ✅ 수정 3: 데이터 행 파싱 - 날짜 정규식으로 필터링
    const revenueData = [];
    
    for (let i = 3; i < rows.length; i++) {
      const row = rows[i];
      if (!row || row.length < 5) continue;
      
      // ✅ 날짜 유효성 검사 (현재까지, 누적 행 제외)
      const dateStr = row[COL.날짜];
      if (!isValidDateRow(dateStr)) {
        console.log(`  ⏭️ 건너뜀 (유효하지 않은 날짜): "${dateStr}"`);
        continue;
      }
      
      // ✅ 수정: 모든 수익 컬럼 합산으로 total 계산
      let total = 0;
      for (const colIdx of COL.수익컬럼들) {
        const val = parseNumber(row[colIdx]);
        total += val;
      }
      
      if (total === 0) continue;

      // breakdown - 모든 수익 카테고리 저장
      const breakdown = {
        래플응모: COL.래플응모 >= 0 ? parseNumber(row[COL.래플응모]) : 0,
        팀워크: COL.팀워크 >= 0 ? parseNumber(row[COL.팀워크]) : 0,
        스팀팩상자개봉: COL.스팀팩상자개봉 >= 0 ? parseNumber(row[COL.스팀팩상자개봉]) : 0,
        신발소켓개방: COL.신발소켓개방 >= 0 ? parseNumber(row[COL.신발소켓개방]) : 0,
        장비소켓개방: COL.장비소켓개방 >= 0 ? parseNumber(row[COL.장비소켓개방]) : 0,
        첫구매패키지: COL.첫구매패키지 >= 0 ? parseNumber(row[COL.첫구매패키지]) : 0,
        슈퍼즈응원단슬롯개방: COL.슈퍼즈응원단슬롯개방 >= 0 ? parseNumber(row[COL.슈퍼즈응원단슬롯개방]) : 0,
        슈퍼즈캔디구매: COL.슈퍼즈캔디구매 >= 0 ? parseNumber(row[COL.슈퍼즈캔디구매]) : 0,
        확률구매신발: COL.확률구매신발 >= 0 ? parseNumber(row[COL.확률구매신발]) : 0,
        확률구매슈퍼즈: COL.확률구매슈퍼즈 >= 0 ? parseNumber(row[COL.확률구매슈퍼즈]) : 0,
        거래수수료신발: COL.거래수수료신발 >= 0 ? parseNumber(row[COL.거래수수료신발]) : 0,
        거래수수료슈퍼즈: COL.거래수수료슈퍼즈 >= 0 ? parseNumber(row[COL.거래수수료슈퍼즈]) : 0,
        이벤트상점: COL.이벤트상점 >= 0 ? parseNumber(row[COL.이벤트상점]) : 0,
        특가상품: COL.특가상품 >= 0 ? parseNumber(row[COL.특가상품]) : 0,
        자동수리패스: COL.자동수리패스 >= 0 ? parseNumber(row[COL.자동수리패스]) : 0,
        자동멈춤패스: COL.자동멈춤패스 >= 0 ? parseNumber(row[COL.자동멈춤패스]) : 0,
        옵션보관함A: COL.옵션보관함A >= 0 ? parseNumber(row[COL.옵션보관함A]) : 0,
        옵션보관함B: COL.옵션보관함B >= 0 ? parseNumber(row[COL.옵션보관함B]) : 0,
        옵션보관함C: COL.옵션보관함C >= 0 ? parseNumber(row[COL.옵션보관함C]) : 0,
        교환수수료: COL.교환수수료 >= 0 ? parseNumber(row[COL.교환수수료]) : 0,
        네트워크: COL.네트워크 >= 0 ? parseNumber(row[COL.네트워크]) : 0,
        직판: COL.직판 >= 0 ? parseNumber(row[COL.직판]) : 0,
        공략집: COL.공략집 >= 0 ? parseNumber(row[COL.공략집]) : 0,
        배경화면: COL.배경화면 >= 0 ? parseNumber(row[COL.배경화면]) : 0,
      };

      const dayData = {
        date: dateStr,
        total: total,
        breakdown: breakdown,
      };
      
      revenueData.push(dayData);
      console.log(`  📅 ${dateStr}: ${formatWon(total)}`);
    }

    console.log(`📊 파싱된 매출 데이터: ${revenueData.length}일`);

    if (revenueData.length === 0) {
      console.log('⚠️ 파싱된 데이터 없음');
      return null;
    }

    // 최신 날짜순 정렬
    revenueData.sort((a, b) => {
      const dateA = new Date(a.date);
      const dateB = new Date(b.date);
      return dateB - dateA;
    });

    // 최근 N일 데이터
    const recentData = revenueData.slice(0, days);
    
    // 통계 계산
    const stats = calculateRevenueStats(recentData);

    return {
      data: recentData,
      stats,
      sheetName,
      lastUpdated: recentData[0]?.date || '알 수 없음',
    };
  } catch (error) {
    console.error('Google Sheets 매출 데이터 가져오기 실패:', error.message);
    console.error('상세 에러:', error);
    return null;
  }
}

function parseNumber(str) {
  if (!str || str === '-' || str === '₩') return 0;
  // ₩, 쉼표, 공백 제거 후 숫자로 변환
  const cleaned = String(str).replace(/[₩,\s]/g, '');
  const num = parseInt(cleaned, 10);
  return isNaN(num) ? 0 : num;
}

function calculateRevenueStats(data) {
  if (!data || data.length === 0) return null;

  const totals = data.map(d => d.total);
  const latest = totals[0];      // 가장 최근 (예: 12/21)
  const previous = totals[1] || latest;  // 그 전날 (예: 12/20)
  
  // 7일 평균 계산 (최대 7일)
  const last7Days = totals.slice(0, 7);
  const avg7Day = last7Days.reduce((sum, t) => sum + t, 0) / last7Days.length;

  // ✅ 수정: 카테고리별 - 가장 최근 날짜 기준 (일별 수익원)
  const latestData = data[0];
  const latestBreakdown = latestData?.breakdown || {};

  // 가장 큰 수익원 찾기 (최근 1일 기준)
  const topCategory = Object.entries(latestBreakdown)
    .filter(([_, v]) => v > 0)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 3);

  return {
    latestTotal: latest,
    previousTotal: previous,
    dayOverDayChange: previous > 0 ? ((latest - previous) / previous * 100).toFixed(1) : 0,
    avg7Day: Math.round(avg7Day),
    avgChange: avg7Day > 0 ? ((latest - avg7Day) / avg7Day * 100).toFixed(1) : 0,
    totalPeriod: totals.reduce((sum, t) => sum + t, 0),
    daysCount: data.length,
    topCategories: topCategory,
    latestBreakdown,
  };
}

// ✅ 기존 formatRevenue 함수를 formatWon으로 대체
function formatRevenue(num) {
  return formatWon(num);
}

// ============================================
// Slack 채널 메시지 수집 (스레드 포함)
// ============================================
async function getSlackMessages(days = 1) {
  try {
    const now = Math.floor(Date.now() / 1000);
    const oldest = now - (86400 * days);

    const channelsResult = await slack.conversations.list({
      types: 'public_channel,private_channel',
      exclude_archived: true,
    });

    // 먼저 사용자 목록 가져오기
    const usersResult = await slack.users.list();
    const userMap = {};
    usersResult.members.forEach(user => {
      userMap[user.id] = user.real_name || user.name;
    });

    let allMessages = [];

    for (const channel of channelsResult.channels) {
      try {
        const history = await slack.conversations.history({
          channel: channel.id,
          oldest: oldest,
          latest: now,
          limit: 200,
        });

        for (const msg of history.messages) {
          // 메인 메시지 추가
          const mainMessage = {
            channel: channel.name,
            user: msg.user,
            userName: userMap[msg.user] || '알 수 없음',
            text: msg.text,
            timestamp: msg.ts,
            isThread: false,
            replyCount: msg.reply_count || 0,
          };
          allMessages.push(mainMessage);

          // 스레드가 있으면 스레드 답글도 가져오기
          if (msg.thread_ts && msg.reply_count > 0) {
            try {
              const replies = await slack.conversations.replies({
                channel: channel.id,
                ts: msg.thread_ts,
                limit: 100,
              });

              // 첫 번째는 원본 메시지이므로 제외하고 답글만
              for (const reply of replies.messages.slice(1)) {
                allMessages.push({
                  channel: channel.name,
                  user: reply.user,
                  userName: userMap[reply.user] || '알 수 없음',
                  text: reply.text,
                  timestamp: reply.ts,
                  isThread: true,
                  parentText: msg.text?.slice(0, 50) + '...',
                });
              }
            } catch (err) {
              console.log(`스레드 가져오기 실패:`, err.message);
            }
          }
        }

        // Rate limit 방지
        await new Promise(resolve => setTimeout(resolve, 200));

      } catch (err) {
        console.log(`채널 ${channel.name} 접근 불가:`, err.message);
      }
    }

    return { messages: allMessages, userMap };
  } catch (error) {
    console.error('Slack 메시지 가져오기 실패:', error);
    return { messages: [], userMap: {} };
  }
}

// ============================================
// CEO DM 수집 (스레드 포함)
// ============================================
async function getCEODirectMessages(userMap, days = 1) {
  try {
    const now = Math.floor(Date.now() / 1000);
    const oldest = now - (86400 * days);

    const dmsResult = await slackUser.conversations.list({
      types: 'im',
      limit: 100,
    });

    let allDMs = [];

    for (const dm of dmsResult.channels) {
      try {
        const history = await slackUser.conversations.history({
          channel: dm.id,
          oldest: oldest,
          latest: now,
          limit: 500,
        });

        if (history.messages && history.messages.length > 0) {
          const otherUserId = dm.user;
          const otherUserName = userMap[otherUserId] || '알 수 없음';

          for (const msg of history.messages) {
            // 메인 DM 메시지
            allDMs.push({
              channel: `DM:${otherUserName}`,
              user: msg.user,
              userName: userMap[msg.user] || '알 수 없음',
              text: msg.text,
              timestamp: msg.ts,
              isDM: true,
              isThread: false,
              replyCount: msg.reply_count || 0,
            });

            // DM 스레드가 있으면 스레드 답글도 가져오기
            if (msg.thread_ts && msg.reply_count > 0) {
              try {
                const replies = await slackUser.conversations.replies({
                  channel: dm.id,
                  ts: msg.thread_ts,
                  limit: 100,
                });

                for (const reply of replies.messages.slice(1)) {
                  allDMs.push({
                    channel: `DM:${otherUserName}`,
                    user: reply.user,
                    userName: userMap[reply.user] || '알 수 없음',
                    text: reply.text,
                    timestamp: reply.ts,
                    isDM: true,
                    isThread: true,
                    parentText: msg.text?.slice(0, 50) + '...',
                  });
                }
              } catch (err) {
                // 스레드 접근 실패는 조용히 넘어감
              }
            }
          }
        }

        // Rate limit 방지
        await new Promise(resolve => setTimeout(resolve, 100));

      } catch (err) {
        // DM 접근 실패는 조용히 넘어감
      }
    }

    return allDMs;
  } catch (error) {
    console.error('CEO DM 가져오기 실패:', error);
    return [];
  }
}

// ============================================
// Notion 데이터 수집
// ============================================
async function getRecentNotionPages(days = 1) {
  try {
    const since = new Date(Date.now() - (86400000 * days)).toISOString();
    
    const response = await notion.search({
      filter: {
        property: 'object',
        value: 'page',
      },
      sort: {
        direction: 'descending',
        timestamp: 'last_edited_time',
      },
      page_size: 50,
    });

    // 기간 내 수정된 페이지만 필터링
    const recentPages = response.results.filter(page => {
      return page.last_edited_time >= since;
    });

    const pagesWithContent = [];

    for (const page of recentPages.slice(0, 20)) {
      try {
        const pageInfo = await getPageInfo(page);
        if (pageInfo) {
          pagesWithContent.push(pageInfo);
        }
      } catch (err) {
        console.log(`페이지 정보 가져오기 실패:`, err.message);
      }
    }

    return pagesWithContent;
  } catch (error) {
    console.error('Notion 페이지 가져오기 실패:', error);
    return [];
  }
}

async function getPageInfo(page) {
  try {
    // 페이지 제목 추출
    let title = '제목 없음';
    if (page.properties) {
      const titleProp = Object.values(page.properties).find(
        prop => prop.type === 'title'
      );
      if (titleProp && titleProp.title && titleProp.title[0]) {
        title = titleProp.title[0].plain_text;
      }
    }

    // 페이지 내용 가져오기
    const blocks = await notion.blocks.children.list({
      block_id: page.id,
      page_size: 20,
    });

    let content = '';
    for (const block of blocks.results) {
      const text = extractTextFromBlock(block);
      if (text) {
        content += text + '\n';
      }
    }

    // 댓글 가져오기
    let comments = [];
    try {
      const commentsResponse = await notion.comments.list({
        block_id: page.id,
      });
      comments = commentsResponse.results.map(comment => ({
        author: comment.created_by?.id || 'unknown',
        text: comment.rich_text?.map(t => t.plain_text).join('') || '',
        createdAt: comment.created_time,
      }));
    } catch (err) {
      // 댓글 접근 실패는 조용히 넘어감
    }

    return {
      id: page.id,
      title,
      content: content.slice(0, 1000),
      lastEditedTime: page.last_edited_time,
      lastEditedBy: page.last_edited_by?.id || 'unknown',
      comments,
    };
  } catch (error) {
    console.error('페이지 정보 가져오기 실패:', error);
    return null;
  }
}

function extractTextFromBlock(block) {
  const type = block.type;
  const content = block[type];
  
  if (!content) return '';
  
  if (content.rich_text) {
    return content.rich_text.map(t => t.plain_text).join('');
  }
  
  return '';
}

async function getNotionDatabases(days = 1) {
  try {
    const since = new Date(Date.now() - (86400000 * days)).toISOString();
    
    const response = await notion.search({
      filter: {
        property: 'object',
        value: 'database',
      },
      page_size: 20,
    });

    const databaseSummaries = [];

    for (const db of response.results) {
      try {
        // 데이터베이스 제목
        let dbTitle = '제목 없음';
        if (db.title && db.title[0]) {
          dbTitle = db.title[0].plain_text;
        }

        // 최근 수정된 항목 가져오기
        const items = await notion.databases.query({
          database_id: db.id,
          filter: {
            timestamp: 'last_edited_time',
            last_edited_time: {
              on_or_after: since,
            },
          },
          page_size: 10,
        });

        if (items.results.length > 0) {
          const itemSummaries = items.results.map(item => {
            const titleProp = Object.values(item.properties).find(
              p => p.type === 'title'
            );
            const title = titleProp?.title?.[0]?.plain_text || '제목 없음';

            const statusProp = Object.values(item.properties).find(
              p => p.type === 'status' || p.type === 'select'
            );
            const status = statusProp?.status?.name || 
                          statusProp?.select?.name || '';

            return { title, status };
          });

          databaseSummaries.push({
            name: dbTitle,
            recentItems: itemSummaries,
            totalUpdated: items.results.length,
          });
        }
      } catch (err) {
        console.log(`데이터베이스 쿼리 실패:`, err.message);
      }
    }

    return databaseSummaries;
  } catch (error) {
    console.error('Notion 데이터베이스 가져오기 실패:', error);
    return [];
  }
}

async function getNotionUsers() {
  try {
    const response = await notion.users.list();
    const userMap = {};
    
    response.results.forEach(user => {
      userMap[user.id] = user.name || user.id;
    });

    return userMap;
  } catch (error) {
    console.error('Notion 사용자 목록 가져오기 실패:', error);
    return {};
  }
}

// ============================================
// Claude 분석
// ============================================
async function analyzeWithClaude(slackMessages, ceoDMs, notionData, revenueData, days = 1) {
  const { pages, databases, users } = notionData;
  const isInitialRun = days > 1;

  // Slack 채널 메시지 포맷팅 (스레드 표시 추가)
  let slackSection = '메시지 없음';
  if (slackMessages.length > 0) {
    slackSection = slackMessages
      .map(m => {
        const threadTag = m.isThread ? '  ↳ [스레드]' : '';
        const replyInfo = m.replyCount > 0 ? ` (답글 ${m.replyCount}개)` : '';
        return `${threadTag}[${m.channel}] ${m.userName}: ${m.text}${replyInfo}`;
      })
      .join('\n');
  }

  // CEO DM 포맷팅 (스레드 표시 추가)
  let dmSection = 'DM 없음';
  if (ceoDMs.length > 0) {
    dmSection = ceoDMs
      .map(m => {
        const threadTag = m.isThread ? '  ↳ [스레드]' : '';
        const replyInfo = m.replyCount > 0 ? ` (답글 ${m.replyCount}개)` : '';
        return `${threadTag}[${m.channel}] ${m.userName}: ${m.text}${replyInfo}`;
      })
      .join('\n');
  }

  // Notion 페이지 포맷팅 (댓글 강조)
  let notionPagesSection = '업데이트된 페이지 없음';
  if (pages.length > 0) {
    notionPagesSection = pages
      .map(p => {
        const editor = users[p.lastEditedBy] || '알 수 없음';
        let section = `📄 ${p.title} (수정: ${editor})\n내용: ${p.content.slice(0, 500)}`;
        if (p.comments.length > 0) {
          section += `\n💬 댓글 (${p.comments.length}개):\n`;
          section += p.comments.map(c => 
            `  - ${users[c.author] || '익명'}: ${c.text}`
          ).join('\n');
        }
        return section;
      })
      .join('\n\n');
  }

  // Notion 데이터베이스 포맷팅
  let notionDbSection = '업데이트된 데이터베이스 없음';
  if (databases.length > 0) {
    notionDbSection = databases
      .map(db => {
        const items = db.recentItems
          .map(item => `  - ${item.title}${item.status ? ` [${item.status}]` : ''}`)
          .join('\n');
        return `📊 ${db.name} (${db.totalUpdated}개 업데이트)\n${items}`;
      })
      .join('\n\n');
  }

  // ✅ 수정 6: 매출 데이터 포맷팅 (formatWon 적용)
  let revenueSection = '매출 데이터 없음 (시트 미연동 또는 데이터 없음)';
  if (revenueData && revenueData.data && revenueData.data.length > 0) {
    const stats = revenueData.stats;
    const recentDays = revenueData.data.slice(0, 7);
    
    // ✅ 전일대비 변화 계산
    const latestTotal = stats.latestTotal;
    const previousTotal = stats.previousTotal;
    const diff = latestTotal - previousTotal;
    const diffSign = diff >= 0 ? '+' : '';
    
    // 최근 날짜 (어제)
    const latestDate = revenueData.data[0]?.date || '알 수 없음';
    const previousDate = revenueData.data[1]?.date || '알 수 없음';
    
    revenueSection = `📊 매출 현황 (${revenueData.sheetName} 시트)

💰 어제(${latestDate}) 매출: ${formatWon(stats.latestTotal)}
  - 전일(${previousDate}) 매출: ${formatWon(stats.previousTotal)}
  - 전일 대비: ${diffSign}${formatWon(Math.abs(diff))} (${stats.dayOverDayChange > 0 ? '+' : ''}${stats.dayOverDayChange}%)
  - 7일 평균: ${formatWon(stats.avg7Day)} (평균 대비 ${stats.avgChange > 0 ? '+' : ''}${stats.avgChange}%)

🏆 어제 Top 수익원:
${stats.topCategories.map(([cat, val]) => `  - ${cat}: ${formatWon(val)}`).join('\n')}

📅 최근 7일 매출:
${recentDays.map(d => `  ${d.date}: ${formatWon(d.total)}`).join('\n')}`;
  }

  // 초기 분석용 vs 일일 분석용 프롬프트
  const analysisFormat = isInitialRun ? `
다음 형식으로 ${days}일간의 종합 분석을 해주세요:

💰 매출 트렌드 분석
   - 기간 내 매출 추이
   - 급증/급락 구간 및 추정 원인
   - 수익원별 비중 변화
   - Slack/Notion 논의 내용과 매출 연관성

👥 팀원별 커뮤니케이션 패턴
   - 각 팀원과의 DM 빈도 및 주요 논의 주제
   - 소통이 잘 되는 팀원 vs 관심 필요한 팀원
   - 1:1 미팅 우선순위 추천

🔥 주요 이슈 타임라인
   - 기간 내 반복적으로 등장한 문제들
   - 해결된 이슈 vs 아직 열린 이슈
   - 에스컬레이션 필요한 사항

📝 Notion 활동 분석
   - 활발히 업데이트된 문서/프로젝트
   - 문서화가 부족한 영역
   - Slack 대화 vs Notion 문서 갭
   - ⚠️ 중요 댓글/피드백 하이라이트

📊 조직 건강도 진단
   - 소통 병목 구간
   - 의사결정 지연 패턴
   - 팀 간 협업 상태

💡 CEO 액션 아이템 (우선순위순)
   1. 즉시 처리 필요
   2. 이번 주 내 처리
   3. 모니터링 필요

🎯 앞으로의 모니터링 포인트
   - 특히 주시해야 할 팀원/프로젝트
   - 예상되는 리스크` : `
다음 형식으로 분석해주세요:

💰 매출 현황 요약
   - 어제 매출 및 전일/평균 대비 변화
   - 주목할 수익원 변화
   - Slack/DM에서 논의된 매출 관련 이슈

📌 긴급 이슈 (우선순위 Top 3)
🔴 [출처: 채널/DM/Notion] [팀명] 이슈 제목
   - 상황: 간단 요약
   - 영향: 비즈니스 임팩트
   - 추천 액션: CEO가 할 일

🟡 주의 필요
   (같은 형식)

💬 DM 팔로업 필요
   - 누구와의 대화인지
   - 약속/결정 사항
   - 후속 조치 필요한 것
   - ⚠️ 스레드에서 나온 중요 맥락 포함

📝 Notion 주요 변경
   - 중요 문서 업데이트
   - 프로젝트 상태 변경
   - ⚠️ 주목할 댓글/피드백 (누가 뭐라고 했는지)

🟢 칭찬할 점 / 좋은 진행상황
   - 팀원 이름
   - 기여 내용
   - 추천 액션

⚠️ 패턴 감지
   - 반복되는 문제
   - 소통 단절 징후 (Slack ↔ Notion 불일치)
   - DM에서만 나온 이슈 (채널 공유 필요?)
   - 스레드에서 논의 중인데 결론 없는 건들
   - 매출 변동과 연관된 논의

📊 생산성 인사이트
   - 가장 활발한 팀원/채널
   - 1:1 미팅 필요해 보이는 팀원
   - 스레드 논의가 길어지는 주제 (미팅 필요?)`;

  const prompt = `당신은 CEO의 Staff로서 조직을 모니터링합니다.
${isInitialRun ? `\n🚀 이것은 최초 분석입니다. 지난 ${days}일간의 데이터를 종합적으로 분석해주세요.\n` : ''}

⚠️ 중요: 
- 스레드 답글과 Notion 댓글에 핵심 맥락이 담겨 있습니다
- 매출 데이터와 Slack/Notion 논의를 교차 분석해서 인사이트를 도출하세요
- [스레드] 표시가 있는 메시지는 원본 메시지에 대한 답글입니다

═══════════════════════════════════
💰 SuperWalk 매출 데이터 (Google Sheets)
═══════════════════════════════════
${revenueSection}

═══════════════════════════════════
📱 Slack 채널 대화 + 스레드 (${days}일)
═══════════════════════════════════
${slackSection}

═══════════════════════════════════
💬 CEO 1:1 DM 대화 + 스레드 (${days}일)
═══════════════════════════════════
${dmSection}

═══════════════════════════════════
📝 Notion 페이지 업데이트 + 댓글
═══════════════════════════════════
${notionPagesSection}

═══════════════════════════════════
📊 Notion 데이터베이스 변경
═══════════════════════════════════
${notionDbSection}

═══════════════════════════════════
${analysisFormat}

분석 시 주의사항:
- Slack, DM, Notion, 매출 데이터 교차 분석
- 매출 변동이 있으면 관련 Slack/DM 논의 찾아서 연결
- ⭐ 스레드/댓글에서 나온 논의 포인트 놓치지 않기
- DM 내용은 민감할 수 있으니 팩트 중심으로
- 비즈니스 임팩트가 큰 것 우선
- 구체적 액션 아이템
- SuperWalk/DeFi/베이직 모드 관련 특히 주의`;

  try {
    const message = await anthropic.messages.create({
      model: 'claude-sonnet-4-20250514',
      max_tokens: isInitialRun ? 4000 : 3000,
      messages: [
        {
          role: 'user',
          content: prompt,
        },
      ],
    });

    return message.content[0].text;
  } catch (error) {
    console.error('Claude 분석 실패:', error);
    return '분석 중 오류가 발생했습니다.';
  }
}

// ============================================
// CEO에게 DM 발송
// ============================================
async function sendDMToCEO(analysis, stats) {
  try {
    const isInitial = stats.days > 1;
    const headerText = isInitial 
      ? `🚀 ${stats.days}일간 종합 분석 리포트`
      : '📊 어제의 조직 모니터링 리포트';
    
    // 스레드/매출 포함 표시
    let statsText = `📈 수집 (${stats.days}일): Slack ${stats.slackCount}개 | DM ${stats.dmCount}개 | Notion ${stats.notionPages}개`;
    if (stats.revenueDataAvailable) {
      statsText += ` | 💰 매출 데이터 포함`;
    }

    await slack.chat.postMessage({
      channel: process.env.CEO_SLACK_ID,
      text: `${headerText}\n\n${analysis}`,
      blocks: [
        {
          type: 'header',
          text: {
            type: 'plain_text',
            text: headerText,
            emoji: true,
          },
        },
        {
          type: 'context',
          elements: [
            {
              type: 'mrkdwn',
              text: statsText,
            },
          ],
        },
        {
          type: 'divider',
        },
        {
          type: 'section',
          text: {
            type: 'mrkdwn',
            text: analysis.slice(0, 3000),
          },
        },
        {
          type: 'divider',
        },
        {
          type: 'context',
          elements: [
            {
              type: 'mrkdwn',
              text: `🕐 ${new Date().toLocaleString('ko-KR')} | 🤖 Claude Sonnet 4`,
            },
          ],
        },
      ],
    });

    if (analysis.length > 3000) {
      await slack.chat.postMessage({
        channel: process.env.CEO_SLACK_ID,
        text: analysis.slice(3000),
      });
    }

    console.log('CEO에게 DM 발송 완료');
  } catch (error) {
    console.error('DM 발송 실패:', error);
  }
}

// ============================================
// 메인 핸들러
// ============================================
module.exports = async (req, res) => {
  // req.query가 없을 때 기본값 처리
  const days = Math.min(parseInt(req.query?.days || req.body?.days) || 1, 30);
  const isInitialRun = days > 1;

  console.log('='.repeat(50));
  console.log(`${isInitialRun ? '🚀 초기 분석' : '📅 정기 분석'} 시작`);
  console.log(`📆 분석 기간: ${days}일`);
  console.log('✅ 스레드/댓글/매출 데이터 수집 포함');
  console.log('='.repeat(50));

  try {
    // 0. 매출 데이터 수집 (Google Sheets)
    console.log('\n💰 매출 데이터 수집 중 (Google Sheets)...');
    const revenueData = await getRevenueData(Math.max(days, 7));
    if (revenueData) {
      console.log(`✅ 매출 데이터: ${revenueData.data.length}일치 (최종: ${revenueData.lastUpdated})`);
    } else {
      console.log('⚠️ 매출 데이터 없음 (계속 진행)');
    }

    // 1. Slack 채널 메시지 수집 (스레드 포함)
    console.log('\n📱 Slack 채널 메시지 + 스레드 수집 중...');
    const { messages: slackMessages, userMap } = await getSlackMessages(days);
    const threadCount = slackMessages.filter(m => m.isThread).length;
    console.log(`✅ Slack: ${slackMessages.length}개 (스레드 ${threadCount}개 포함)`);

    // 2. CEO DM 수집 (스레드 포함)
    console.log('\n💬 CEO DM + 스레드 수집 중...');
    const ceoDMs = await getCEODirectMessages(userMap, days);
    const dmThreadCount = ceoDMs.filter(m => m.isThread).length;
    console.log(`✅ CEO DM: ${ceoDMs.length}개 (스레드 ${dmThreadCount}개 포함)`);

    // 3. Notion 사용자
    console.log('\n👥 Notion 사용자 목록...');
    const notionUsers = await getNotionUsers();
    console.log(`✅ Notion 사용자: ${Object.keys(notionUsers).length}명`);

    // 4. Notion 페이지 (댓글 포함)
    console.log('\n📝 Notion 페이지 + 댓글 수집 중...');
    const notionPages = await getRecentNotionPages(days);
    const commentCount = notionPages.reduce((sum, p) => sum + p.comments.length, 0);
    console.log(`✅ Notion 페이지: ${notionPages.length}개 (댓글 ${commentCount}개 포함)`);

    // 5. Notion 데이터베이스
    console.log('\n📊 Notion 데이터베이스 수집 중...');
    const notionDatabases = await getNotionDatabases(days);
    console.log(`✅ Notion DB: ${notionDatabases.length}개`);

    // 6. Claude 분석 (매출 데이터 포함)
    console.log('\n🤖 Claude 분석 중...');
    const analysis = await analyzeWithClaude(slackMessages, ceoDMs, {
      pages: notionPages,
      databases: notionDatabases,
      users: notionUsers,
    }, revenueData, days);
    console.log('✅ 분석 완료');

    // 7. CEO에게 발송
    console.log('\n📤 CEO에게 DM 발송 중...');
    await sendDMToCEO(analysis, {
      slackCount: slackMessages.length,
      dmCount: ceoDMs.length,
      notionPages: notionPages.length,
      notionDbs: notionDatabases.length,
      days: days,
      revenueDataAvailable: !!revenueData,
    });

    console.log('\n✅ 완료!');

    res.status(200).json({
      success: true,
      type: isInitialRun ? 'initial_analysis' : 'daily_analysis',
      days: days,
      stats: {
        slackMessages: slackMessages.length,
        slackThreads: threadCount,
        ceoDMs: ceoDMs.length,
        dmThreads: dmThreadCount,
        notionPages: notionPages.length,
        notionComments: commentCount,
        notionDatabases: notionDatabases.length,
        revenueData: revenueData ? {
          days: revenueData.data.length,
          lastUpdated: revenueData.lastUpdated,
          latestTotal: revenueData.stats?.latestTotal,
        } : null,
      },
      timestamp: new Date().toISOString(),
    });
  } catch (error) {
    console.error('❌ 실패:', error);
    res.status(500).json({
      success: false,
      error: error.message,
      timestamp: new Date().toISOString(),
    });
  }
};
