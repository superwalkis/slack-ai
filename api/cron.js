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
// 날짜 유틸리티 함수들
// ============================================
function getKSTDate(date = new Date()) {
  // UTC+9 한국 시간으로 변환
  const kstOffset = 9 * 60 * 60 * 1000;
  return new Date(date.getTime() + kstOffset);
}

function formatDateString(date) {
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, '0');
  const day = String(date.getDate()).padStart(2, '0');
  return `${year}-${month}-${day}`;
}

function getYesterdayDateString() {
  const kstNow = getKSTDate();
  const yesterday = new Date(kstNow);
  yesterday.setDate(yesterday.getDate() - 1);
  return formatDateString(yesterday);
}

function isValidDateRow(dateStr) {
  if (!dateStr) return false;
  const value = String(dateStr).trim();
  if (value === '' || value.includes('현재까지') || value.includes('누적')) return false;
  return /^\d{4}-\d{2}-\d{2}$/.test(value);
}

// ============================================
// 금액 포맷팅 (억/만원 단위)
// ============================================
function formatWon(amount) {
  if (!amount || amount === 0) return '₩0';

  if (amount >= 100_000_000) {
    const v = (amount / 100_000_000).toFixed(1);
    return `₩${v}억`;
  }

  if (amount >= 10_000) {
    const v = (amount / 10_000).toFixed(1);
    return `₩${v}만`;
  }

  return '₩' + amount.toLocaleString('ko-KR');
}

function parseNumber(str) {
  if (!str || str === '-' || str === '₩' || str === '') return 0;
  const cleaned = String(str).replace(/[₩,\s]/g, '');
  const num = parseInt(cleaned, 10);
  return isNaN(num) ? 0 : num;
}

// ============================================
// Google Sheets 매출 데이터 수집 (개선됨)
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
    
    // 한국 시간 기준 현재 월
    const kstNow = getKSTDate();
    const sheetName = `${String(kstNow.getFullYear()).slice(2)}.${String(kstNow.getMonth() + 1).padStart(2, '0')}`;
    
    console.log(`📊 시트 이름: ${sheetName}`);
    console.log(`📅 한국 시간: ${kstNow.toISOString()}`);
    
    // 전체 범위 가져오기 (A:AD까지 - 날짜 컬럼 AC 포함)
    const range = `${sheetName}!A:AD`;
    
    const response = await sheets.spreadsheets.values.get({
      spreadsheetId,
      range,
    });

    const rows = response.data.values;
    if (!rows || rows.length < 4) {
      console.log('매출 데이터 없음 - 행 수:', rows?.length || 0);
      return null;
    }

    // 헤더 분석 (2번째 행이 헤더)
    const headers = rows[1] || [];
    console.log('📋 헤더:', headers.slice(0, 10).join(', '), '...');
    
    // 날짜 컬럼과 합계 컬럼 찾기
    let dateColIndex = -1;
    let totalColIndex = -1;
    
    headers.forEach((header, idx) => {
      if (!header) return;
      const h = String(header).trim();
      if (h === '날짜') dateColIndex = idx;
      if (h === '합계') totalColIndex = idx;
    });
    
    console.log(`📍 날짜 컬럼: ${dateColIndex}, 합계 컬럼: ${totalColIndex}`);
    
    // 날짜 컬럼이 없으면 마지막 컬럼 근처에서 찾기 (AC열 = 28)
    if (dateColIndex === -1) {
      // AC열(28번 인덱스) 확인
      for (let i = 25; i < Math.min(headers.length + 5, 35); i++) {
        // 데이터 행에서 날짜 형식 찾기
        for (let rowIdx = 3; rowIdx < Math.min(rows.length, 10); rowIdx++) {
          const cell = rows[rowIdx]?.[i];
          if (cell && isValidDateRow(cell)) {
            dateColIndex = i;
            console.log(`📍 날짜 컬럼 발견 (데이터 기반): ${dateColIndex}`);
            break;
          }
        }
        if (dateColIndex !== -1) break;
      }
    }
    
    // 합계 컬럼이 없으면 날짜 컬럼 바로 앞에서 찾기
    if (totalColIndex === -1 && dateColIndex > 0) {
      totalColIndex = dateColIndex - 1;
      console.log(`📍 합계 컬럼 추정 (날짜 앞): ${totalColIndex}`);
    }

    // 개별 수익 카테고리 컬럼 찾기 (기존 로직 유지)
    const findCol = (keywords) => {
      return headers.findIndex(h => h && keywords.some(k => String(h).includes(k)));
    };

    const categoryColumns = {
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
    };

    // 데이터 수집
    const revenueData = [];
    const yesterdayStr = getYesterdayDateString();
    console.log(`📅 어제 날짜 (기대값): ${yesterdayStr}`);
    
    for (let i = 3; i < rows.length; i++) {
      const row = rows[i];
      if (!row || row.length < 5) continue;
      
      // 날짜 가져오기
      const dateStr = dateColIndex >= 0 ? row[dateColIndex] : null;
      if (!isValidDateRow(dateStr)) continue;
      
      // 합계 가져오기 (합계 컬럼에서 직접 가져오기)
      let total = 0;
      if (totalColIndex >= 0) {
        total = parseNumber(row[totalColIndex]);
      }
      
      // 합계가 0이면 데이터 없는 날로 처리 (하지만 기록은 남김)
      const hasData = total > 0;
      
      // 개별 카테고리 breakdown
      const breakdown = {};
      for (const [category, colIdx] of Object.entries(categoryColumns)) {
        if (colIdx >= 0) {
          breakdown[category] = parseNumber(row[colIdx]);
        } else {
          breakdown[category] = 0;
        }
      }

      const dayData = {
        date: dateStr,
        total: total,
        hasData: hasData,
        breakdown: breakdown,
      };
      
      revenueData.push(dayData);
      
      // 디버깅: 최근 7일 데이터 출력
      if (revenueData.length <= 7) {
        console.log(`  ${dateStr}: ${formatWon(total)} ${hasData ? '✓' : '(데이터 없음)'}`);
      }
    }

    if (revenueData.length === 0) {
      return null;
    }

    // 날짜 내림차순 정렬
    revenueData.sort((a, b) => new Date(b.date) - new Date(a.date));

    // 어제 데이터 확인
    const yesterdayData = revenueData.find(d => d.date === yesterdayStr);
    const hasYesterdayData = yesterdayData && yesterdayData.hasData;
    
    // 가장 최근 유효 데이터 찾기
    const latestValidData = revenueData.find(d => d.hasData);
    
    console.log(`📊 어제(${yesterdayStr}) 데이터: ${hasYesterdayData ? formatWon(yesterdayData.total) : '없음'}`);
    console.log(`📊 최신 유효 데이터: ${latestValidData ? `${latestValidData.date} - ${formatWon(latestValidData.total)}` : '없음'}`);

    // 유효한 데이터만 필터링해서 통계 계산
    const validData = revenueData.filter(d => d.hasData);
    const recentValidData = validData.slice(0, days);
    const stats = calculateRevenueStats(recentValidData);

    return {
      data: recentValidData,
      allData: revenueData,
      stats,
      sheetName,
      lastUpdated: latestValidData?.date || '알 수 없음',
      yesterdayStr,
      hasYesterdayData,
      yesterdayTotal: hasYesterdayData ? yesterdayData.total : null,
    };
  } catch (error) {
    console.error('Google Sheets 매출 데이터 가져오기 실패:', error.message);
    return null;
  }
}

function calculateRevenueStats(data) {
  if (!data || data.length === 0) return null;

  const totals = data.map(d => d.total);
  const latest = totals[0];
  const previous = totals[1] || latest;
  
  const last7Days = totals.slice(0, 7);
  const avg7Day = last7Days.length > 0 
    ? last7Days.reduce((sum, t) => sum + t, 0) / last7Days.length 
    : 0;

  const latestData = data[0];
  const latestBreakdown = latestData?.breakdown || {};

  const topCategory = Object.entries(latestBreakdown)
    .filter(([_, v]) => v > 0)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 5);

  // 7일 카테고리별 합계 (트렌드 파악용)
  const categoryTotals = {};
  data.slice(0, 7).forEach(d => {
    Object.entries(d.breakdown).forEach(([cat, val]) => {
      categoryTotals[cat] = (categoryTotals[cat] || 0) + val;
    });
  });

  return {
    latestDate: latestData?.date,
    latestTotal: latest,
    previousDate: data[1]?.date,
    previousTotal: previous,
    dayOverDayChange: previous > 0 ? ((latest - previous) / previous * 100).toFixed(1) : 0,
    dayOverDayDiff: latest - previous,
    avg7Day: Math.round(avg7Day),
    avgChange: avg7Day > 0 ? ((latest - avg7Day) / avg7Day * 100).toFixed(1) : 0,
    totalPeriod: totals.reduce((sum, t) => sum + t, 0),
    daysCount: data.length,
    topCategories: topCategory,
    latestBreakdown,
    categoryTotals,
  };
}

// ============================================
// Slack 채널 메시지 수집 (스레드 강화)
// ============================================
async function getSlackMessages(days = 1) {
  try {
    const now = Math.floor(Date.now() / 1000);
    const oldest = now - (86400 * days);

    const channelsResult = await slack.conversations.list({
      types: 'public_channel,private_channel',
      exclude_archived: true,
    });

    const usersResult = await slack.users.list();
    const userMap = {};
    usersResult.members.forEach(user => {
      userMap[user.id] = user.real_name || user.name;
    });

    let allMessages = [];
    let threadCount = 0;

    for (const channel of channelsResult.channels) {
      try {
        const history = await slack.conversations.history({
          channel: channel.id,
          oldest: oldest,
          latest: now,
          limit: 200,
        });

        for (const msg of history.messages) {
          const mainMessage = {
            channel: channel.name,
            user: msg.user,
            userName: userMap[msg.user] || '알 수 없음',
            text: msg.text,
            timestamp: msg.ts,
            isThread: false,
            replyCount: msg.reply_count || 0,
            threadTs: msg.thread_ts,
          };
          allMessages.push(mainMessage);

          // 개선: 스레드가 있으면 무조건 가져오기 (reply_count 체크 완화)
          // thread_ts가 있으면 스레드 부모이거나 스레드에 속한 메시지
          if (msg.thread_ts) {
            try {
              const replies = await slack.conversations.replies({
                channel: channel.id,
                ts: msg.thread_ts,
                limit: 200, // 100 -> 200으로 증가
              });

              // 첫 번째 메시지(부모)를 제외한 모든 답글
              for (const reply of replies.messages.slice(1)) {
                // 중복 방지: 이미 수집된 메시지인지 확인
                const isDuplicate = allMessages.some(
                  m => m.timestamp === reply.ts && m.channel === channel.name
                );
                
                if (!isDuplicate) {
                  allMessages.push({
                    channel: channel.name,
                    user: reply.user,
                    userName: userMap[reply.user] || '알 수 없음',
                    text: reply.text,
                    timestamp: reply.ts,
                    isThread: true,
                    parentTs: msg.thread_ts,
                    parentText: msg.text?.slice(0, 50) + '...',
                  });
                  threadCount++;
                }
              }
            } catch (err) {
              console.log(`스레드 접근 실패 (${channel.name}):`, err.message);
            }
          }
        }

        await new Promise(resolve => setTimeout(resolve, 200));

      } catch (err) {
        console.log(`채널 접근 불가 (${channel.name}):`, err.message);
      }
    }

    console.log(`📧 스레드 댓글 수집: ${threadCount}개`);
    return { messages: allMessages, userMap };
  } catch (error) {
    console.error('Slack 메시지 가져오기 실패:', error);
    return { messages: [], userMap: {} };
  }
}

// ============================================
// CEO DM 수집 (스레드 강화)
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
    let threadCount = 0;

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

            // 개선: 스레드 무조건 확인
            if (msg.thread_ts) {
              try {
                const replies = await slackUser.conversations.replies({
                  channel: dm.id,
                  ts: msg.thread_ts,
                  limit: 200,
                });

                for (const reply of replies.messages.slice(1)) {
                  const isDuplicate = allDMs.some(
                    m => m.timestamp === reply.ts && m.channel === `DM:${otherUserName}`
                  );
                  
                  if (!isDuplicate) {
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
                    threadCount++;
                  }
                }
              } catch (err) {
                // 스레드 접근 실패
              }
            }
          }
        }

        await new Promise(resolve => setTimeout(resolve, 100));

      } catch (err) {
        // DM 접근 실패
      }
    }

    console.log(`💬 DM 스레드 댓글 수집: ${threadCount}개`);
    return allDMs;
  } catch (error) {
    console.error('CEO DM 가져오기 실패:', error);
    return [];
  }
}

// ============================================
// Notion 데이터 수집 (깊이 강화)
// ============================================
async function getRecentNotionPages(days = 1) {
  try {
    const since = new Date(Date.now() - (86400000 * days)).toISOString();
    
    // 1. 검색으로 최근 수정된 페이지 가져오기
    const response = await notion.search({
      filter: {
        property: 'object',
        value: 'page',
      },
      sort: {
        direction: 'descending',
        timestamp: 'last_edited_time',
      },
      page_size: 100, // 50 -> 100으로 증가
    });

    const recentPages = response.results.filter(page => {
      return page.last_edited_time >= since;
    });

    console.log(`📄 Notion 최근 수정 페이지: ${recentPages.length}개`);

    const pagesWithContent = [];

    for (const page of recentPages.slice(0, 30)) { // 20 -> 30으로 증가
      try {
        const pageInfo = await getPageInfoDeep(page);
        if (pageInfo) {
          pagesWithContent.push(pageInfo);
        }
      } catch (err) {
        console.log(`페이지 정보 가져오기 실패 (${page.id}):`, err.message);
      }
    }

    return pagesWithContent;
  } catch (error) {
    console.error('Notion 페이지 가져오기 실패:', error);
    return [];
  }
}

// 개선: 하위 블록까지 재귀적으로 탐색
async function getPageInfoDeep(page) {
  try {
    let title = '제목 없음';
    if (page.properties) {
      const titleProp = Object.values(page.properties).find(
        prop => prop.type === 'title'
      );
      if (titleProp && titleProp.title && titleProp.title[0]) {
        title = titleProp.title[0].plain_text;
      }
    }

    // 재귀적으로 블록 내용 가져오기
    const content = await getBlockContentRecursive(page.id, 2); // depth 2까지

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
      // 댓글 접근 실패
    }

    return {
      id: page.id,
      title,
      content: content.slice(0, 1500), // 1000 -> 1500
      lastEditedTime: page.last_edited_time,
      lastEditedBy: page.last_edited_by?.id || 'unknown',
      comments,
      url: page.url,
    };
  } catch (error) {
    return null;
  }
}

async function getBlockContentRecursive(blockId, maxDepth, currentDepth = 0) {
  if (currentDepth >= maxDepth) return '';
  
  try {
    const blocks = await notion.blocks.children.list({
      block_id: blockId,
      page_size: 50,
    });

    let content = '';
    
    for (const block of blocks.results) {
      const text = extractTextFromBlock(block);
      if (text) {
        const indent = '  '.repeat(currentDepth);
        content += `${indent}${text}\n`;
      }
      
      // 하위 블록이 있으면 재귀 탐색
      if (block.has_children) {
        const childContent = await getBlockContentRecursive(
          block.id, 
          maxDepth, 
          currentDepth + 1
        );
        content += childContent;
      }
    }

    return content;
  } catch (error) {
    return '';
  }
}

function extractTextFromBlock(block) {
  const type = block.type;
  const content = block[type];
  
  if (!content) return '';
  
  if (content.rich_text) {
    const text = content.rich_text.map(t => t.plain_text).join('');
    
    // 블록 타입에 따른 접두사
    switch (type) {
      case 'heading_1':
        return `# ${text}`;
      case 'heading_2':
        return `## ${text}`;
      case 'heading_3':
        return `### ${text}`;
      case 'bulleted_list_item':
        return `• ${text}`;
      case 'numbered_list_item':
        return `- ${text}`;
      case 'to_do':
        const checked = content.checked ? '✓' : '○';
        return `${checked} ${text}`;
      case 'toggle':
        return `▸ ${text}`;
      default:
        return text;
    }
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
      page_size: 30, // 20 -> 30
    });

    const databaseSummaries = [];

    for (const db of response.results) {
      try {
        let dbTitle = '제목 없음';
        if (db.title && db.title[0]) {
          dbTitle = db.title[0].plain_text;
        }

        const items = await notion.databases.query({
          database_id: db.id,
          filter: {
            timestamp: 'last_edited_time',
            last_edited_time: {
              on_or_after: since,
            },
          },
          page_size: 20, // 10 -> 20
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

            // 추가 속성 수집
            const dateProp = Object.values(item.properties).find(
              p => p.type === 'date'
            );
            const date = dateProp?.date?.start || '';

            return { 
              title, 
              status, 
              date,
              lastEdited: item.last_edited_time,
            };
          });

          databaseSummaries.push({
            name: dbTitle,
            recentItems: itemSummaries,
            totalUpdated: items.results.length,
          });
        }
      } catch (err) {
        // 데이터베이스 쿼리 실패
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
    return {};
  }
}

// ============================================
// Claude 분석 (개선된 프롬프트)
// ============================================
async function analyzeWithClaude(slackMessages, ceoDMs, notionData, revenueData, days = 1) {
  const { pages, databases, users } = notionData;

  // Slack 채널 메시지 포맷팅 (시간순 정렬)
  let slackSection = '메시지 없음';
  if (slackMessages.length > 0) {
    const sortedMessages = [...slackMessages].sort((a, b) => 
      parseFloat(a.timestamp) - parseFloat(b.timestamp)
    );
    
    slackSection = sortedMessages
      .map(m => {
        const threadTag = m.isThread ? '  ↳ [스레드]' : '';
        const replyInfo = m.replyCount > 0 ? ` (답글 ${m.replyCount}개)` : '';
        return `${threadTag}[${m.channel}] ${m.userName}: ${m.text}${replyInfo}`;
      })
      .join('\n');
  }

  // CEO DM 포맷팅 (시간순 정렬)
  let dmSection = 'DM 없음';
  if (ceoDMs.length > 0) {
    const sortedDMs = [...ceoDMs].sort((a, b) => 
      parseFloat(a.timestamp) - parseFloat(b.timestamp)
    );
    
    dmSection = sortedDMs
      .map(m => {
        const threadTag = m.isThread ? '  ↳ [스레드]' : '';
        const replyInfo = m.replyCount > 0 ? ` (답글 ${m.replyCount}개)` : '';
        return `${threadTag}[${m.channel}] ${m.userName}: ${m.text}${replyInfo}`;
      })
      .join('\n');
  }

  // Notion 페이지 포맷팅
  let notionPagesSection = '업데이트된 페이지 없음';
  if (pages.length > 0) {
    notionPagesSection = pages
      .map(p => {
        const editor = users[p.lastEditedBy] || '알 수 없음';
        let section = `[${p.title}] (수정: ${editor})\n내용: ${p.content.slice(0, 500)}`;
        if (p.comments.length > 0) {
          section += `\n댓글 (${p.comments.length}개):\n`;
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
          .map(item => `  - ${item.title}${item.status ? ` [${item.status}]` : ''}${item.date ? ` (${item.date})` : ''}`)
          .join('\n');
        return `[${db.name}] (${db.totalUpdated}개 업데이트)\n${items}`;
      })
      .join('\n\n');
  }

  // 매출 데이터 포맷팅 (개선됨)
  let revenueSection = '매출 데이터 없음';
  if (revenueData && revenueData.data && revenueData.data.length > 0) {
    const stats = revenueData.stats;
    const recentDays = revenueData.data.slice(0, 7);
    
    // 어제 데이터 유무 명시
    let yesterdayInfo = '';
    if (revenueData.hasYesterdayData) {
      yesterdayInfo = `어제(${revenueData.yesterdayStr}) 매출: ${formatWon(revenueData.yesterdayTotal)}`;
    } else {
      yesterdayInfo = `⚠ 어제(${revenueData.yesterdayStr}) 데이터 없음\n가장 최근 데이터: ${stats.latestDate} - ${formatWon(stats.latestTotal)}`;
    }
    
    const diff = stats.dayOverDayDiff;
    const diffSign = diff >= 0 ? '+' : '';
    
    revenueSection = `[매출 현황 - ${revenueData.sheetName} 시트]

${yesterdayInfo}
전일(${stats.previousDate}) 매출: ${formatWon(stats.previousTotal)}
전일 대비: ${diffSign}${formatWon(Math.abs(diff))} (${stats.dayOverDayChange > 0 ? '+' : ''}${stats.dayOverDayChange}%)
7일 평균: ${formatWon(stats.avg7Day)} (평균 대비 ${stats.avgChange > 0 ? '+' : ''}${stats.avgChange}%)

최근 데이터 수익원 Top 5:
${stats.topCategories.map(([cat, val]) => `  - ${cat}: ${formatWon(val)}`).join('\n')}

최근 7일 매출:
${recentDays.map(d => `  ${d.date}: ${formatWon(d.total)}`).join('\n')}`;
  }

  // ============================================
  // 개선된 프롬프트
  // ============================================
  const prompt = `당신은 월 2~3억 매출 규모, 성장 과도기에 있는 스타트업 CEO의 Staff입니다.
매일 아침 CEO가 빠르게 읽고 의사결정할 수 있는 브리핑을 작성합니다.

[핵심 원칙]
1. 숫자 정확성: 단위(일간/누적/%), 오해 가능성 있으면 "⚠ 검증 필요" 표시. 추정/해석 금지.
2. 스케일 맥락: 월 2~3억 매출 기준에서 의미 있는 것만. "누가 말을 많이 했다" 같은 건 제외.
3. 액션 중심: 각 항목마다 "그래서 뭘 해야 하는지"가 명확해야 함.
4. 간결함: 과장, 스토리텔링, 이모지 남발 금지. 팩트와 숫자 중심.
5. 스레드 맥락: 스레드 내 대화는 전체 흐름을 파악해서, 최종 결론이나 합의점을 반영하세요. 중간 논의만 보고 판단하지 마세요.

═══════════════════════════════════
[매출 데이터]
═══════════════════════════════════
${revenueSection}

═══════════════════════════════════
[Slack 채널 대화] (시간순, 스레드 포함)
═══════════════════════════════════
${slackSection}

═══════════════════════════════════
[CEO DM 대화] (시간순, 스레드 포함)
═══════════════════════════════════
${dmSection}

═══════════════════════════════════
[Notion 페이지 업데이트]
═══════════════════════════════════
${notionPagesSection}

═══════════════════════════════════
[Notion 데이터베이스 변경]
═══════════════════════════════════
${notionDbSection}

═══════════════════════════════════

아래 형식으로 브리핑을 작성하세요. 볼드(**) 사용하지 마세요.

# CEO 일일 브리핑

## 1) 매출/핵심 KPI
어제: [금액] ([전일대비 %], [7일평균대비 %])
※ 어제 데이터가 없으면 "어제 데이터 없음, 최신: [날짜] [금액]"으로 표기
주요 수익원: [Top 3와 금액]
이상 징후: [있으면 구체적으로, 없으면 "없음"]

## 2) 긴급/리스크 신호 (Top 3)
[출처] 이슈명
- 상황: 1줄 요약
- 왜 중요: 비즈니스 영향
- 즉시 액션: 오늘 할 1가지

※ 스레드 대화에서 이미 해결된 것으로 보이는 이슈는 "[해결됨]" 표시
(최대 3개. 긴급한 게 없으면 "긴급 이슈 없음"으로 끝)

## 3) 전략적으로 중요한 변화
베이직 모드 / 수익모델 / DeFi / 조직 / 교보 관련 실제 진행된 것만.
진행 없으면 "특이사항 없음"

## 4) 팀/조직 인사이트
체크 필요한 사람:
- [이름]: [구체적 상황과 이유]

칭찬할 사람:
- [이름]: [기여 내용]

(의미 있는 행동이 없으면 해당 항목 생략)

## 5) 오늘 결정/실행할 것
즉시: [오늘 중 해야 할 것]
단기(이번주): [이번 주 내 해야 할 것]
중기: [2주 내 해야 할 것]

(각 항목 없으면 생략)

## 6) 무시해도 좋은 것
- [신경 쓸 필요 없는 논의들]

(없으면 이 섹션 생략)

---
[주의사항 다시 한번]
- 숫자 틀리면 안 됨. 확실하지 않으면 "⚠ 확인 필요" 붙이기
- 과장 금지. "매우 중요", "심각한" 같은 수식어 자제
- 볼드(**) 사용 금지
- 이모지는 섹션 제목에만 최소한으로
- 스레드에서 결론 났거나 해결된 건 명시하기`;

  try {
    const message = await anthropic.messages.create({
      model: 'claude-sonnet-4-20250514',
      max_tokens: 2500,
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
    const today = new Date();
    const dateStr = `${today.getMonth() + 1}/${today.getDate()}`;
    const headerText = `📋 CEO 일일 브리핑 (${dateStr})`;
    
    let statsText = `수집: Slack ${stats.slackCount}개 | DM ${stats.dmCount}개 | Notion ${stats.notionPages}개`;
    if (stats.revenueDataAvailable) {
      statsText += ` | 매출 데이터 포함`;
      if (!stats.hasYesterdayData) {
        statsText += ` (어제 데이터 없음)`;
      }
    }
    statsText += ` | 스레드 ${stats.threadCount}개`;

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
              text: `${new Date().toLocaleString('ko-KR')} | Claude Sonnet 4`,
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
  const days = Math.min(parseInt(req.query?.days || req.body?.days) || 1, 30);

  console.log('='.repeat(50));
  console.log(`📋 CEO 일일 브리핑 생성 시작`);
  console.log(`📆 분석 기간: ${days}일`);
  console.log(`📅 현재 시각 (UTC): ${new Date().toISOString()}`);
  console.log(`📅 현재 시각 (KST): ${getKSTDate().toISOString()}`);
  console.log('='.repeat(50));

  try {
    // 0. 매출 데이터 수집
    console.log('\n💰 매출 데이터 수집 중...');
    const revenueData = await getRevenueData(Math.max(days, 7));
    if (revenueData) {
      console.log(`✅ 매출 데이터: ${revenueData.data.length}일치`);
      console.log(`   어제 데이터: ${revenueData.hasYesterdayData ? '있음' : '없음'}`);
    }

    // 1. Slack 채널 메시지 수집
    console.log('\n📱 Slack 메시지 수집 중...');
    const { messages: slackMessages, userMap } = await getSlackMessages(days);
    const slackThreadCount = slackMessages.filter(m => m.isThread).length;
    console.log(`✅ Slack: ${slackMessages.length}개 (스레드 ${slackThreadCount}개)`);

    // 2. CEO DM 수집
    console.log('\n💬 CEO DM 수집 중...');
    const ceoDMs = await getCEODirectMessages(userMap, days);
    const dmThreadCount = ceoDMs.filter(m => m.isThread).length;
    console.log(`✅ CEO DM: ${ceoDMs.length}개 (스레드 ${dmThreadCount}개)`);

    // 3. Notion 사용자
    console.log('\n👥 Notion 사용자 목록...');
    const notionUsers = await getNotionUsers();

    // 4. Notion 페이지
    console.log('\n📝 Notion 페이지 수집 중...');
    const notionPages = await getRecentNotionPages(days);
    console.log(`✅ Notion 페이지: ${notionPages.length}개`);

    // 5. Notion 데이터베이스
    console.log('\n📊 Notion 데이터베이스 수집 중...');
    const notionDatabases = await getNotionDatabases(days);
    console.log(`✅ Notion DB: ${notionDatabases.length}개`);

    // 6. Claude 분석
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
      hasYesterdayData: revenueData?.hasYesterdayData || false,
      threadCount: slackThreadCount + dmThreadCount,
    });

    console.log('\n✅ 완료!');

    res.status(200).json({
      success: true,
      days: days,
      stats: {
        slackMessages: slackMessages.length,
        slackThreads: slackThreadCount,
        ceoDMs: ceoDMs.length,
        dmThreads: dmThreadCount,
        notionPages: notionPages.length,
        notionDatabases: notionDatabases.length,
        revenueData: revenueData ? {
          days: revenueData.data.length,
          latestTotal: revenueData.stats?.latestTotal,
          latestDate: revenueData.stats?.latestDate,
          hasYesterdayData: revenueData.hasYesterdayData,
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
