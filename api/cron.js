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
// 날짜 유효성 검사 함수
// ============================================
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
    
    const now = new Date();
    const sheetName = `${String(now.getFullYear()).slice(2)}.${String(now.getMonth() + 1).padStart(2, '0')}`;
    
    console.log(`📊 시트 이름: ${sheetName}`);
    
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

    const headers = rows[1];

    const findCol = (keywords) => {
      return headers.findIndex(h => h && keywords.some(k => h.includes(k)));
    };

    const revenueColIndexes = [];
    const excludeKeywords = ['날짜', 'GRND', '종가', '소분류'];
    
    headers.forEach((header, idx) => {
      if (!header) return;
      const isExcluded = excludeKeywords.some(k => header.includes(k));
      if (!isExcluded && idx > 0) {
        revenueColIndexes.push(idx);
      }
    });

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

    const revenueData = [];
    
    for (let i = 3; i < rows.length; i++) {
      const row = rows[i];
      if (!row || row.length < 5) continue;
      
      const dateStr = row[COL.날짜];
      if (!isValidDateRow(dateStr)) continue;
      
      let total = 0;
      for (const colIdx of COL.수익컬럼들) {
        const val = parseNumber(row[colIdx]);
        total += val;
      }
      
      if (total === 0) continue;

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
    }

    if (revenueData.length === 0) {
      return null;
    }

    revenueData.sort((a, b) => {
      const dateA = new Date(a.date);
      const dateB = new Date(b.date);
      return dateB - dateA;
    });

    const recentData = revenueData.slice(0, days);
    const stats = calculateRevenueStats(recentData);

    return {
      data: recentData,
      stats,
      sheetName,
      lastUpdated: recentData[0]?.date || '알 수 없음',
    };
  } catch (error) {
    console.error('Google Sheets 매출 데이터 가져오기 실패:', error.message);
    return null;
  }
}

function parseNumber(str) {
  if (!str || str === '-' || str === '₩') return 0;
  const cleaned = String(str).replace(/[₩,\s]/g, '');
  const num = parseInt(cleaned, 10);
  return isNaN(num) ? 0 : num;
}

function calculateRevenueStats(data) {
  if (!data || data.length === 0) return null;

  const totals = data.map(d => d.total);
  const latest = totals[0];
  const previous = totals[1] || latest;
  
  const last7Days = totals.slice(0, 7);
  const avg7Day = last7Days.reduce((sum, t) => sum + t, 0) / last7Days.length;

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
    latestTotal: latest,
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

          if (msg.thread_ts && msg.reply_count > 0) {
            try {
              const replies = await slack.conversations.replies({
                channel: channel.id,
                ts: msg.thread_ts,
                limit: 100,
              });

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
              // 스레드 접근 실패
            }
          }
        }

        await new Promise(resolve => setTimeout(resolve, 200));

      } catch (err) {
        // 채널 접근 불가
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
        // 페이지 정보 가져오기 실패
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
    let title = '제목 없음';
    if (page.properties) {
      const titleProp = Object.values(page.properties).find(
        prop => prop.type === 'title'
      );
      if (titleProp && titleProp.title && titleProp.title[0]) {
        title = titleProp.title[0].plain_text;
      }
    }

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
      content: content.slice(0, 1000),
      lastEditedTime: page.last_edited_time,
      lastEditedBy: page.last_edited_by?.id || 'unknown',
      comments,
    };
  } catch (error) {
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

  // Slack 채널 메시지 포맷팅
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

  // CEO DM 포맷팅
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
          .map(item => `  - ${item.title}${item.status ? ` [${item.status}]` : ''}`)
          .join('\n');
        return `[${db.name}] (${db.totalUpdated}개 업데이트)\n${items}`;
      })
      .join('\n\n');
  }

  // 매출 데이터 포맷팅
  let revenueSection = '매출 데이터 없음';
  if (revenueData && revenueData.data && revenueData.data.length > 0) {
    const stats = revenueData.stats;
    const recentDays = revenueData.data.slice(0, 7);
    
    const latestDate = revenueData.data[0]?.date || '알 수 없음';
    const previousDate = revenueData.data[1]?.date || '알 수 없음';
    const diff = stats.dayOverDayDiff;
    const diffSign = diff >= 0 ? '+' : '';
    
    revenueSection = `[매출 현황 - ${revenueData.sheetName} 시트]

어제(${latestDate}) 매출: ${formatWon(stats.latestTotal)}
전일(${previousDate}) 매출: ${formatWon(stats.previousTotal)}
전일 대비: ${diffSign}${formatWon(Math.abs(diff))} (${stats.dayOverDayChange > 0 ? '+' : ''}${stats.dayOverDayChange}%)
7일 평균: ${formatWon(stats.avg7Day)} (평균 대비 ${stats.avgChange > 0 ? '+' : ''}${stats.avgChange}%)

어제 수익원 Top 5:
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

═══════════════════════════════════
[매출 데이터]
═══════════════════════════════════
${revenueSection}

═══════════════════════════════════
[Slack 채널 대화]
═══════════════════════════════════
${slackSection}

═══════════════════════════════════
[CEO DM 대화]
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
주요 수익원: [Top 3와 금액]
이상 징후: [있으면 구체적으로, 없으면 "없음"]

## 2) 긴급/리스크 신호 (Top 3)
[출처] 이슈명
- 상황: 1줄 요약
- 왜 중요: 비즈니스 영향
- 즉시 액션: 오늘 할 1가지

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
- 이모지는 섹션 제목에만 최소한으로`;

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
  console.log('='.repeat(50));

  try {
    // 0. 매출 데이터 수집
    console.log('\n💰 매출 데이터 수집 중...');
    const revenueData = await getRevenueData(Math.max(days, 7));
    if (revenueData) {
      console.log(`✅ 매출 데이터: ${revenueData.data.length}일치`);
    }

    // 1. Slack 채널 메시지 수집
    console.log('\n📱 Slack 메시지 수집 중...');
    const { messages: slackMessages, userMap } = await getSlackMessages(days);
    console.log(`✅ Slack: ${slackMessages.length}개`);

    // 2. CEO DM 수집
    console.log('\n💬 CEO DM 수집 중...');
    const ceoDMs = await getCEODirectMessages(userMap, days);
    console.log(`✅ CEO DM: ${ceoDMs.length}개`);

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
    });

    console.log('\n✅ 완료!');

    res.status(200).json({
      success: true,
      days: days,
      stats: {
        slackMessages: slackMessages.length,
        ceoDMs: ceoDMs.length,
        notionPages: notionPages.length,
        notionDatabases: notionDatabases.length,
        revenueData: revenueData ? {
          days: revenueData.data.length,
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
