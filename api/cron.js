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
// 로깅 유틸리티
// ============================================
const LOG_LEVELS = {
  DEBUG: 0,
  INFO: 1,
  WARN: 2,
  ERROR: 3,
};

const currentLogLevel = LOG_LEVELS.INFO;

function log(level, category, message, data = null) {
  if (LOG_LEVELS[level] >= currentLogLevel) {
    const timestamp = new Date().toISOString();
    const prefix = {
      DEBUG: '🔍',
      INFO: '📌',
      WARN: '⚠️',
      ERROR: '❌',
    }[level];
    
    console.log(`${prefix} [${timestamp}] [${category}] ${message}`);
    if (data && level === 'DEBUG') {
      console.log(JSON.stringify(data, null, 2).slice(0, 500));
    }
  }
}

// ============================================
// CEO 명언 목록
// ============================================
const CEO_QUOTES = [
  { quote: "Your most unhappy customers are your greatest source of learning.", author: "Bill Gates" },
  { quote: "If you're not embarrassed by the first version of your product, you've launched too late.", author: "Reid Hoffman" },
  { quote: "Move fast and break things. Unless you are breaking stuff, you are not moving fast enough.", author: "Mark Zuckerberg" },
  { quote: "The way to get started is to quit talking and begin doing.", author: "Walt Disney" },
  { quote: "I think frugality drives innovation, just like other constraints do.", author: "Jeff Bezos" },
  { quote: "Stay hungry, stay foolish.", author: "Steve Jobs" },
  { quote: "In the end, a vision without the ability to execute it is probably a hallucination.", author: "Steve Case" },
  { quote: "The biggest risk is not taking any risk.", author: "Mark Zuckerberg" },
  { quote: "Success is a lousy teacher. It seduces smart people into thinking they can't lose.", author: "Bill Gates" },
  { quote: "If you double the number of experiments you do per year, you're going to double your inventiveness.", author: "Jeff Bezos" },
  { quote: "People who are crazy enough to think they can change the world are the ones who do.", author: "Steve Jobs" },
  { quote: "The secret to successful hiring is this: look for the people who want to change the world.", author: "Marc Benioff" },
  { quote: "It's fine to celebrate success but it is more important to heed the lessons of failure.", author: "Bill Gates" },
  { quote: "Life is too short to hang out with people who aren't resourceful.", author: "Jeff Bezos" },
  { quote: "Don't let the noise of others' opinions drown out your own inner voice.", author: "Steve Jobs" },
  { quote: "The only way to do great work is to love what you do.", author: "Steve Jobs" },
  { quote: "Ideas are easy. Implementation is hard.", author: "Guy Kawasaki" },
  { quote: "Make every detail perfect and limit the number of details to perfect.", author: "Jack Dorsey" },
  { quote: "If you're competitor-focused, you have to wait until there is a competitor doing something.", author: "Jeff Bezos" },
  { quote: "Chase the vision, not the money; the money will end up following you.", author: "Tony Hsieh" },
  { quote: "The best time to repair the roof is when the sun is shining.", author: "John F. Kennedy" },
  { quote: "Culture eats strategy for breakfast.", author: "Peter Drucker" },
  { quote: "What gets measured gets managed.", author: "Peter Drucker" },
  { quote: "Speed is the ultimate weapon in business.", author: "Jack Welch" },
  { quote: "Transparency breeds legitimacy.", author: "John Donahoe" },
];

function getRandomQuote() {
  return CEO_QUOTES[Math.floor(Math.random() * CEO_QUOTES.length)];
}

// ============================================
// 날짜 유틸리티 함수들
// ============================================
function getKSTDate(date = new Date()) {
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

function getDayOfWeek(date) {
  const days = ['일', '월', '화', '수', '목', '금', '토'];
  return days[date.getDay()];
}

// ============================================
// 금액 포맷팅
// ============================================
function formatWon(amount) {
  if (!amount || amount === 0) return '₩0';
  const absAmount = Math.abs(amount);
  const sign = amount < 0 ? '-' : '';
  if (absAmount >= 100_000_000) {
    return `${sign}₩${(absAmount / 100_000_000).toFixed(1)}억`;
  }
  if (absAmount >= 10_000) {
    return `${sign}₩${(absAmount / 10_000).toFixed(0)}만`;
  }
  return sign + '₩' + absAmount.toLocaleString('ko-KR');
}

function parseNumber(str) {
  if (!str || str === '-' || str === '₩' || str === '') return 0;
  const cleaned = String(str).replace(/[₩,\s]/g, '');
  const num = parseInt(cleaned, 10);
  return isNaN(num) ? 0 : num;
}

// ============================================
// Google Calendar 일정 수집 (주황색 미팅만 필터)
// ============================================
async function getCalendarEvents(daysBack = 1, daysForward = 7) {
  try {
    const credentials = JSON.parse(process.env.GOOGLE_SERVICE_ACCOUNT_JSON || '{}');
    
    if (!credentials.client_email) {
      log('INFO', 'Calendar', 'Google 서비스 계정 미설정 - 캘린더 스킵');
      return null;
    }

    const ceoEmail = process.env.CEO_GOOGLE_EMAIL;
    if (!ceoEmail) {
      log('INFO', 'Calendar', 'CEO_GOOGLE_EMAIL 미설정 - 캘린더 스킵');
      return null;
    }

    const auth = new google.auth.GoogleAuth({
      credentials,
      scopes: ['https://www.googleapis.com/auth/calendar.readonly'],
    });

    const authClient = await auth.getClient();
    authClient.subject = ceoEmail;

    const calendar = google.calendar({ version: 'v3', auth: authClient });

    const now = new Date();
    const timeMin = new Date(now.getTime() - (daysBack * 24 * 60 * 60 * 1000));
    const timeMax = new Date(now.getTime() + (daysForward * 24 * 60 * 60 * 1000));

    const response = await calendar.events.list({
      calendarId: ceoEmail,
      timeMin: timeMin.toISOString(),
      timeMax: timeMax.toISOString(),
      singleEvents: true,
      orderBy: 'startTime',
      maxResults: 50,
    });

    const events = response.data.items || [];
    
    const todayMeetings = [];
    const upcomingMeetings = [];
    
    const kstNow = new Date(now.getTime() + (9 * 60 * 60 * 1000));
    const todayStart = new Date(kstNow);
    todayStart.setUTCHours(0 - 9, 0, 0, 0);
    const todayEnd = new Date(kstNow);
    todayEnd.setUTCHours(23 - 9, 59, 59, 999);

    for (const event of events) {
      // 주황색(colorId '6')만 필터링 - 실제 미팅
      if (event.colorId !== '6') continue;
      
      const start = new Date(event.start?.dateTime || event.start?.date);
      const end = new Date(event.end?.dateTime || event.end?.date);
      
      // 미팅 타입 구분
      let meetingType = '내부';
      if (event.location) {
        meetingType = '외부';
      } else if (event.hangoutLink || (event.description && /zoom|meet\.google|teams/i.test(event.description))) {
        meetingType = '외부-화상';
      }
      
      const eventData = {
        id: event.id,
        title: event.summary || '제목 없음',
        start: start,
        end: end,
        startStr: event.start?.dateTime 
          ? start.toLocaleString('ko-KR', { timeZone: 'Asia/Seoul', hour: 'numeric', minute: '2-digit', hour12: false })
          : formatDateString(start),
        duration: Math.round((end - start) / (1000 * 60)),
        location: event.location || '',
        description: event.description || '',
        attendees: (event.attendees || []).map(a => ({
          email: a.email,
          name: a.displayName || a.email.split('@')[0],
        })),
        meetLink: event.hangoutLink || '',
        meetingType: meetingType,
      };

      if (start >= todayStart && start <= todayEnd) {
        todayMeetings.push(eventData);
      } else if (start > todayEnd) {
        upcomingMeetings.push(eventData);
      }
    }

    log('INFO', 'Calendar', `캘린더: 오늘 미팅 ${todayMeetings.length}건, 예정 ${upcomingMeetings.length}건`);

    return {
      today: todayMeetings,
      upcoming: upcomingMeetings,
    };
  } catch (error) {
    log('ERROR', 'Calendar', `Google Calendar 가져오기 실패: ${error.message}`);
    return null;
  }
}

// ============================================
// Google Sheets 매출 데이터 수집
// ============================================
async function getRevenueData(days = 7) {
  try {
    const credentials = JSON.parse(process.env.GOOGLE_SERVICE_ACCOUNT_JSON || '{}');
    
    if (!credentials.client_email) {
      log('INFO', 'Revenue', 'Google 서비스 계정 미설정 - 매출 데이터 스킵');
      return null;
    }

    const auth = new google.auth.GoogleAuth({
      credentials,
      scopes: ['https://www.googleapis.com/auth/spreadsheets.readonly'],
    });

    const sheets = google.sheets({ version: 'v4', auth });
    const spreadsheetId = '1e97jBZ9tSsJ0RiU8aGwp_t6w5RW-5olZ8G1fLYhTy8g';
    
    const kstNow = getKSTDate();
    const sheetName = `${String(kstNow.getFullYear()).slice(2)}.${String(kstNow.getMonth() + 1).padStart(2, '0')}`;
    
    const range = `${sheetName}!A:AD`;
    
    const response = await sheets.spreadsheets.values.get({
      spreadsheetId,
      range,
    });

    const rows = response.data.values;
    if (!rows || rows.length < 4) {
      return null;
    }

    const headers = rows[1] || [];
    
    let dateColIndex = -1;
    let totalColIndex = -1;
    
    headers.forEach((header, idx) => {
      if (!header) return;
      const h = String(header).trim();
      if (h === '날짜') dateColIndex = idx;
      if (h === '합계') totalColIndex = idx;
    });
    
    if (dateColIndex === -1) {
      for (let i = 25; i < Math.min(headers.length + 5, 35); i++) {
        for (let rowIdx = 3; rowIdx < Math.min(rows.length, 10); rowIdx++) {
          const cell = rows[rowIdx]?.[i];
          if (cell && isValidDateRow(cell)) {
            dateColIndex = i;
            break;
          }
        }
        if (dateColIndex !== -1) break;
      }
    }
    
    if (totalColIndex === -1 && dateColIndex > 0) {
      totalColIndex = dateColIndex - 1;
    }

    const revenueData = [];
    const yesterdayStr = getYesterdayDateString();
    
    for (let i = 3; i < rows.length; i++) {
      const row = rows[i];
      if (!row || row.length < 5) continue;
      
      const dateStr = dateColIndex >= 0 ? row[dateColIndex] : null;
      if (!isValidDateRow(dateStr)) continue;
      
      let total = 0;
      if (totalColIndex >= 0) {
        total = parseNumber(row[totalColIndex]);
      }
      
      const hasData = total > 0;

      revenueData.push({
        date: dateStr,
        total,
        hasData,
      });
    }

    if (revenueData.length === 0) return null;

    revenueData.sort((a, b) => new Date(b.date) - new Date(a.date));

    const yesterdayData = revenueData.find(d => d.date === yesterdayStr);
    const hasYesterdayData = yesterdayData && yesterdayData.hasData;
    const latestValidData = revenueData.find(d => d.hasData);
    const validData = revenueData.filter(d => d.hasData);
    
    const monthlyTarget = parseInt(process.env.MONTHLY_REVENUE_TARGET) || 200_000_000;
    const currentMonth = kstNow.getMonth() + 1;
    const daysInMonth = new Date(kstNow.getFullYear(), currentMonth, 0).getDate();
    const currentDay = kstNow.getDate();
    const remainingDays = daysInMonth - currentDay + 1;
    
    const mtdRevenue = validData
      .filter(d => d.date.startsWith(`${kstNow.getFullYear()}-${String(currentMonth).padStart(2, '0')}`))
      .reduce((sum, d) => sum + d.total, 0);
    
    const targetProgress = (mtdRevenue / monthlyTarget * 100).toFixed(1);
    
    const last7DaysAvg = validData.slice(0, 7).reduce((sum, d) => sum + d.total, 0) / Math.min(7, validData.length);
    const projectedMonthEnd = mtdRevenue + (last7DaysAvg * remainingDays);

    const latestTotal = latestValidData?.total || 0;
    const previousTotal = validData[1]?.total || latestTotal;
    const dayOverDayChange = previousTotal > 0 ? ((latestTotal - previousTotal) / previousTotal * 100).toFixed(1) : 0;

    return {
      data: validData.slice(0, days),
      sheetName,
      lastUpdated: latestValidData?.date || '알 수 없음',
      yesterdayStr,
      hasYesterdayData,
      yesterdayTotal: hasYesterdayData ? yesterdayData.total : latestValidData?.total,
      latestDate: latestValidData?.date,
      stats: {
        latestTotal,
        previousTotal,
        dayOverDayChange,
        avg7Day: Math.round(last7DaysAvg),
      },
      monthlyAnalysis: {
        target: monthlyTarget,
        mtd: mtdRevenue,
        progress: parseFloat(targetProgress),
        remainingDays,
        projectedMonthEnd: Math.round(projectedMonthEnd),
        onTrack: projectedMonthEnd >= monthlyTarget * 0.9,
      },
    };
  } catch (error) {
    log('ERROR', 'Revenue', `Google Sheets 매출 데이터 가져오기 실패: ${error.message}`);
    return null;
  }
}

// ============================================
// [NEW] 1Q 목표 시트 데이터 수집
// ============================================
async function getQuarterlyTargetData() {
  try {
    const credentials = JSON.parse(process.env.GOOGLE_SERVICE_ACCOUNT_JSON || '{}');
    
    if (!credentials.client_email) {
      log('INFO', 'Target', 'Google 서비스 계정 미설정 - 목표 데이터 스킵');
      return null;
    }

    const auth = new google.auth.GoogleAuth({
      credentials,
      scopes: ['https://www.googleapis.com/auth/spreadsheets.readonly'],
    });

    const sheets = google.sheets({ version: 'v4', auth });
    const spreadsheetId = '1Vm5hi9Dwqx7OGErtz6f8PrJpegWJdKZKCwaDUAr-oc8';
    
    // 전체 시트 데이터 가져오기
    const range = '시트1!A:G';
    
    const response = await sheets.spreadsheets.values.get({
      spreadsheetId,
      range,
    });

    const rows = response.data.values;
    if (!rows || rows.length < 10) {
      log('WARN', 'Target', '목표 시트 데이터 부족');
      return null;
    }

    // 현재 월 확인 (1월 = B열, 2월 = C열, ...)
    const kstNow = getKSTDate();
    const currentMonth = kstNow.getMonth() + 1; // 1-12
    const currentMonthColIndex = currentMonth; // B=1, C=2, ...
    
    // 데이터 파싱 (행 번호는 0-indexed)
    // Row 3 (index 2): 매출
    // Row 4 (index 3): 비용
    // Row 5 (index 4): 영업 손익
    // Row 7 (index 6): 재무 손익
    // Row 8 (index 7): 캐시플랜(자금조달)
    // Row 9 (index 8): 월말잔고
    
    const getVal = (rowIdx, colIdx) => {
      const val = rows[rowIdx]?.[colIdx];
      return parseNumber(val);
    };

    const currentMonthData = {
      revenue: getVal(2, currentMonthColIndex),
      cost: getVal(3, currentMonthColIndex),
      operatingProfit: getVal(4, currentMonthColIndex),
      financialProfit: getVal(6, currentMonthColIndex),
      fundraising: getVal(7, currentMonthColIndex),
      monthEndBalance: getVal(8, currentMonthColIndex),
    };

    // 1Q 합계 (1월~3월)
    const q1Data = {
      revenue: getVal(2, 1) + getVal(2, 2) + getVal(2, 3),
      cost: getVal(3, 1) + getVal(3, 2) + getVal(3, 3),
      operatingProfit: getVal(4, 1) + getVal(4, 2) + getVal(4, 3),
      fundraising: getVal(7, 1) + getVal(7, 2) + getVal(7, 3),
    };

    // SuperWalk Pro/Basic 세부 (Row 14-15, index 13-14)
    const superwalkPro = getVal(13, currentMonthColIndex);
    const superwalkBasic = getVal(14, currentMonthColIndex);

    // 손익 (Row 45-46, index 44-45)
    const profitPro = getVal(44, currentMonthColIndex);
    const profitBasic = getVal(45, currentMonthColIndex);

    log('INFO', 'Target', `1Q 목표 데이터 로드 완료 - ${currentMonth}월`);

    return {
      currentMonth: {
        month: currentMonth,
        ...currentMonthData,
        superwalkPro,
        superwalkBasic,
        profitPro,
        profitBasic,
      },
      q1: q1Data,
      raw: rows,
    };
  } catch (error) {
    log('ERROR', 'Target', `1Q 목표 시트 가져오기 실패: ${error.message}`);
    return null;
  }
}

// ============================================
// Slack 메시지 수집
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
          allMessages.push({
            channel: channel.name,
            user: msg.user,
            userName: userMap[msg.user] || '알 수 없음',
            text: msg.text,
            timestamp: msg.ts,
            isThread: false,
            replyCount: msg.reply_count || 0,
          });

          if (msg.thread_ts && msg.reply_count > 0) {
            try {
              const replies = await slack.conversations.replies({
                channel: channel.id,
                ts: msg.thread_ts,
                limit: 50,
              });

              for (const reply of replies.messages.slice(1)) {
                allMessages.push({
                  channel: channel.name,
                  user: reply.user,
                  userName: userMap[reply.user] || '알 수 없음',
                  text: reply.text,
                  timestamp: reply.ts,
                  isThread: true,
                });
              }
            } catch (err) {}
          }
        }

        await new Promise(resolve => setTimeout(resolve, 200));
      } catch (err) {}
    }

    return { messages: allMessages, userMap };
  } catch (error) {
    log('ERROR', 'Slack', `Slack 메시지 가져오기 실패: ${error.message}`);
    return { messages: [], userMap: {} };
  }
}

// ============================================
// CEO DM 수집
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
          limit: 200,
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
            });
          }
        }

        await new Promise(resolve => setTimeout(resolve, 100));
      } catch (err) {}
    }

    return allDMs;
  } catch (error) {
    log('ERROR', 'Slack', `CEO DM 가져오기 실패: ${error.message}`);
    return [];
  }
}

// ============================================
// Notion 수집 (간소화 버전)
// ============================================
const notionStats = {
  searchApiPages: 0,
  childPagesFound: 0,
  dbItemsWithContent: 0,
};

async function getRecentNotionPagesDeep(days = 1) {
  const allPages = [];
  const since = new Date(Date.now() - (86400000 * days)).toISOString();
  
  try {
    const searchResults = await notion.search({
      filter: { property: 'object', value: 'page' },
      sort: { direction: 'descending', timestamp: 'last_edited_time' },
      page_size: 30,
    });
    
    const recentFromSearch = searchResults.results.filter(p => p.last_edited_time >= since);
    notionStats.searchApiPages = recentFromSearch.length;
    
    for (const page of recentFromSearch.slice(0, 15)) {
      let title = '제목 없음';
      if (page.properties) {
        const titleProp = Object.values(page.properties).find(prop => prop.type === 'title');
        if (titleProp?.title?.[0]) title = titleProp.title[0].plain_text;
      }
      
      allPages.push({
        id: page.id,
        title,
        lastEditedTime: page.last_edited_time,
      });
    }
    
    log('INFO', 'Notion', `Notion 페이지 ${allPages.length}개 수집`);
  } catch (error) {
    log('ERROR', 'Notion', `Notion 수집 실패: ${error.message}`);
  }
  
  return {
    pages: allPages,
    stats: notionStats,
  };
}

// ============================================
// Claude 분석 (새 템플릿)
// ============================================
async function analyzeWithClaude(slackMessages, ceoDMs, notionData, revenueData, calendarData, targetData, days = 1) {
  const { pages } = notionData;
  const quote = getRandomQuote();
  
  const kstNow = getKSTDate();
  const dateStr = `${kstNow.getMonth() + 1}월 ${kstNow.getDate()}일 ${getDayOfWeek(kstNow)}요일`;

  // Slack 요약
  let slackSummary = '메시지 없음';
  if (slackMessages.length > 0) {
    const sorted = [...slackMessages].sort((a, b) => parseFloat(a.timestamp) - parseFloat(b.timestamp));
    slackSummary = sorted.slice(-50).map(m => {
      const threadTag = m.isThread ? '  ↳' : '';
      return `${threadTag}[${m.channel}] ${m.userName}: ${m.text}`;
    }).join('\n');
  }

  // DM 요약
  let dmSummary = 'DM 없음';
  if (ceoDMs.length > 0) {
    const sorted = [...ceoDMs].sort((a, b) => parseFloat(a.timestamp) - parseFloat(b.timestamp));
    dmSummary = sorted.slice(-30).map(m => `[${m.channel}] ${m.userName}: ${m.text}`).join('\n');
  }

  // Notion 요약
  let notionSummary = '업데이트 없음';
  if (pages.length > 0) {
    notionSummary = pages.map(p => `- ${p.title} (${p.lastEditedTime})`).join('\n');
  }

  // 매출 데이터 요약
  let revenueSummary = '매출 데이터 없음';
  if (revenueData) {
    const r = revenueData;
    const sign = parseFloat(r.stats.dayOverDayChange) >= 0 ? '+' : '';
    revenueSummary = `어제(${r.latestDate}): ${formatWon(r.yesterdayTotal)} (전일비 ${sign}${r.stats.dayOverDayChange}%)
MTD: ${formatWon(r.monthlyAnalysis.mtd)} / ${formatWon(r.monthlyAnalysis.target)} (${r.monthlyAnalysis.progress}%)
7일 평균: ${formatWon(r.stats.avg7Day)}
월말 예상: ${formatWon(r.monthlyAnalysis.projectedMonthEnd)} ${r.monthlyAnalysis.onTrack ? '' : '⚠️ 목표 미달 예상'}`;
  }

  // 1Q 목표 데이터 요약
  let targetSummary = '목표 데이터 없음';
  if (targetData) {
    const t = targetData.currentMonth;
    targetSummary = `${t.month}월 목표:
- 매출 목표: ${formatWon(t.revenue)}
- 영업손익 목표: ${formatWon(t.operatingProfit)}
- 캐시플랜(자금조달): ${formatWon(t.fundraising)}
- 월말잔고 목표: ${formatWon(t.monthEndBalance)}
- SuperWalk Pro 목표: ${formatWon(t.superwalkPro)}
- SuperWalk Basic 목표: ${formatWon(t.superwalkBasic)}

1Q 전체 목표:
- 매출: ${formatWon(targetData.q1.revenue)}
- 자금조달: ${formatWon(targetData.q1.fundraising)}`;
  }

  // 오늘 미팅 요약
  let meetingSummary = '미팅 없음';
  if (calendarData?.today?.length > 0) {
    meetingSummary = calendarData.today.map(m => {
      const attendees = m.attendees.length > 0 ? m.attendees.map(a => a.name).join(', ') : '';
      return `- ${m.startStr} ${m.title} [${m.meetingType}]${attendees ? ` (${attendees})` : ''}
  설명: ${m.description || '없음'}`;
    }).join('\n');
  }

  const prompt = `당신은 Web3 스타트업 CEO의 Chief of Staff입니다.
아래 데이터를 기반으로 CEO가 아침에 3분 안에 읽고 바로 행동할 수 있는 간결한 브리핑을 작성하세요.

[CEO 컨텍스트]
- 교보생명 PoC 데드라인: 1월 13일 (D-6)
- 최근 구조조정 완료 (23명 → 17명)
- 2026년 목표: MAU 300K, 월 광고매출 3-4억, Q4 흑자전환
- 성향: 데이터 기반, 직접적 피드백 선호

[오늘 날짜]
${dateStr}

[명언]
"${quote.quote}" — ${quote.author}

═══════════════════════════════════
[매출 현황]
${revenueSummary}

[1Q 목표 시트]
${targetSummary}

[오늘 미팅 (주황색 일정만)]
${meetingSummary}

[Slack 대화]
${slackSummary.slice(0, 3000)}

[CEO DM]
${dmSummary.slice(0, 1500)}

[Notion 업데이트]
${notionSummary}
═══════════════════════════════════

아래 형식으로 브리핑을 작성하세요. **볼드 사용 금지**, 간결하게.

---

## 🚀 Tim CEO Morning Brief (${dateStr})

> "[상황 요약 - 한 줄로 오늘의 핵심 메시지]"
> 
> *"${quote.quote}"* — ${quote.author}

---

### ⚡️ Today's Focus Mode: [전투/방어/사색 중 택1]

"[오늘 모드에 맞는 한 줄 조언]"

- [영역1] ([N]%): [핵심 행동]
- [영역2] ([N]%): [핵심 행동]  
- [영역3] ([N]%): [핵심 행동]

---

### 📊 Key Metrics

매출 현황
- 어제: [금액] (전일비 [+/-N]%)
- MTD: [금액] / [목표] ([N]%)
- 전망: [예상 금액] [달성가능/⚠️ 목표 미달]

1Q 목표 대비
- [월] 매출 목표: [금액] → 현재 [금액] ([N]%)
- 영업손익 목표: [금액]
- 캐시플랜: [목표금액] 중 [확보금액] 확보
- 월말잔고 목표: [금액]

---

### 🎯 Critical Decisions

1. 🔴 [가장 긴급한 이슈] ([마감시한])
- A) [옵션A] → [결과]
- B) [옵션B] → [결과]
- 👉 추천: [A/B] ([한 줄 근거])

2. 🟡 [두번째 이슈] ([마감시한])
- A) [옵션A] → [결과]
- B) [옵션B] → [결과]
- 👉 추천: [A/B]

3. 🟢 [세번째 이슈] ([마감시한])
- 👉 추천: [권고사항]

(의사결정 필요 없으면 이 섹션 생략)

---

### 📅 Today's Meetings

- [시간] [미팅명] [내부/외부/외부-화상]
  - 목표: [이 미팅에서 얻어야 할 것]

- [시간] [미팅명] [내부/외부/외부-화상]
  - 목표: [이 미팅에서 얻어야 할 것]

(미팅 없으면 "오늘 미팅 없음 - 딥워크 타임 활용하세요")

---

### 🚨 Risk Monitor

- 🔴 [가장 심각한 리스크]: [현황 한 줄]
- 🟡 [주의 필요]: [현황 한 줄]
- 🟢 [안정적]: [현황 한 줄]

---

> 💡 [오늘 CEO가 집중해야 할 핵심 한 줄 요약]

---

[작성 규칙]
- 전체 분량: 최대 800단어
- 볼드(**) 사용 금지
- 모든 금액은 formatWon 형식 (₩2.6억, ₩540만 등)
- 담당자/기한 없는 액션 아이템 금지
- 불확실한 정보는 "⚠️ 확인 필요" 표시`;

  try {
    const message = await anthropic.messages.create({
      model: 'claude-sonnet-4-20250514',
      max_tokens: 2000,
      messages: [{ role: 'user', content: prompt }],
    });

    return message.content[0].text;
  } catch (error) {
    log('ERROR', 'Claude', `Claude 분석 실패: ${error.message}`);
    return '분석 중 오류가 발생했습니다.';
  }
}

// ============================================
// CEO에게 DM 발송
// ============================================
async function sendDMToCEO(analysis, stats) {
  try {
    const kstNow = getKSTDate();
    const dateStr = `${kstNow.getMonth() + 1}/${kstNow.getDate()}`;
    const dayNames = ['일', '월', '화', '수', '목', '금', '토'];
    const dayName = dayNames[kstNow.getDay()];
    const headerText = `🚀 CEO Morning Brief (${dateStr} ${dayName})`;
    
    let statsText = `Slack ${stats.slackCount} | DM ${stats.dmCount} | Notion ${stats.notionPages}`;
    if (stats.revenueDataAvailable) {
      statsText += ` | 매출 ${stats.hasYesterdayData ? '✓' : '⚠️'}`;
    }
    if (stats.targetDataAvailable) {
      statsText += ` | 1Q목표 ✓`;
    }
    if (stats.meetingsCount > 0) {
      statsText += ` | 미팅 ${stats.meetingsCount}건`;
    }

    await slack.chat.postMessage({
      channel: process.env.CEO_SLACK_ID,
      text: `${headerText}\n\n${analysis}`,
      blocks: [
        {
          type: 'header',
          text: { type: 'plain_text', text: headerText, emoji: true },
        },
        {
          type: 'context',
          elements: [{ type: 'mrkdwn', text: statsText }],
        },
        { type: 'divider' },
        {
          type: 'section',
          text: { type: 'mrkdwn', text: analysis.slice(0, 3000) },
        },
      ],
    });

    if (analysis.length > 3000) {
      const remaining = analysis.slice(3000);
      const chunks = remaining.match(/.{1,3000}/g) || [];
      for (const chunk of chunks) {
        await slack.chat.postMessage({
          channel: process.env.CEO_SLACK_ID,
          text: chunk,
        });
      }
    }

    log('INFO', 'Slack', 'CEO에게 DM 발송 완료');
  } catch (error) {
    log('ERROR', 'Slack', `DM 발송 실패: ${error.message}`);
  }
}

// ============================================
// 메인 핸들러
// ============================================
module.exports = async (req, res) => {
  const days = Math.min(parseInt(req.query?.days || req.body?.days) || 1, 30);

  console.log('='.repeat(60));
  log('INFO', 'Main', `CEO Morning Brief 생성 시작`);
  log('INFO', 'Main', `현재 시각 (KST): ${getKSTDate().toISOString()}`);
  console.log('='.repeat(60));

  try {
    // 1. 캘린더 데이터 수집 (주황색 미팅만)
    log('INFO', 'Main', '캘린더 데이터 수집 중...');
    const calendarData = await getCalendarEvents(days, 7);

    // 2. 매출 데이터 수집
    log('INFO', 'Main', '매출 데이터 수집 중...');
    const revenueData = await getRevenueData(Math.max(days, 7));

    // 3. 1Q 목표 데이터 수집
    log('INFO', 'Main', '1Q 목표 데이터 수집 중...');
    const targetData = await getQuarterlyTargetData();

    // 4. Slack 메시지 수집
    log('INFO', 'Main', 'Slack 메시지 수집 중...');
    const { messages: slackMessages, userMap } = await getSlackMessages(days);

    // 5. CEO DM 수집
    log('INFO', 'Main', 'CEO DM 수집 중...');
    const ceoDMs = await getCEODirectMessages(userMap, days);

    // 6. Notion 수집
    log('INFO', 'Main', 'Notion 수집 중...');
    const notionData = await getRecentNotionPagesDeep(days);

    // 7. Claude 분석
    log('INFO', 'Main', 'Claude 분석 중...');
    const analysis = await analyzeWithClaude(
      slackMessages, 
      ceoDMs, 
      notionData,
      revenueData,
      calendarData,
      targetData,
      days
    );

    // 8. CEO에게 발송
    log('INFO', 'Main', 'CEO에게 DM 발송 중...');
    await sendDMToCEO(analysis, {
      slackCount: slackMessages.length,
      dmCount: ceoDMs.length,
      notionPages: notionData.pages.length,
      days,
      revenueDataAvailable: !!revenueData,
      hasYesterdayData: revenueData?.hasYesterdayData || false,
      targetDataAvailable: !!targetData,
      meetingsCount: calendarData?.today?.length || 0,
    });

    log('INFO', 'Main', '완료!');

    res.status(200).json({
      success: true,
      days,
      stats: {
        slackMessages: slackMessages.length,
        ceoDMs: ceoDMs.length,
        notionPages: notionData.pages.length,
        meetings: calendarData?.today?.length || 0,
        revenueAvailable: !!revenueData,
        targetAvailable: !!targetData,
      },
      timestamp: new Date().toISOString(),
    });
  } catch (error) {
    log('ERROR', 'Main', `실패: ${error.message}`);
    console.error(error.stack);
    
    res.status(500).json({
      success: false,
      error: error.message,
      timestamp: new Date().toISOString(),
    });
  }
};
