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
// 로깅 유틸리티 (상세 디버깅용)
// ============================================
const LOG_LEVELS = {
  DEBUG: 0,
  INFO: 1,
  WARN: 2,
  ERROR: 3,
};

const currentLogLevel = LOG_LEVELS.DEBUG; // 디버깅 시 DEBUG, 프로덕션 시 INFO

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

// ============================================
// 금액 포맷팅
// ============================================
function formatWon(amount) {
  if (!amount || amount === 0) return '₩0';
  if (amount >= 100_000_000) {
    return `₩${(amount / 100_000_000).toFixed(1)}억`;
  }
  if (amount >= 10_000) {
    return `₩${(amount / 10_000).toFixed(1)}만`;
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
// Google Calendar 일정 수집
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
    
    const pastEvents = [];
    const todayEvents = [];
    const upcomingEvents = [];
    
    const kstNow = new Date(now.getTime() + (9 * 60 * 60 * 1000));
    const todayStart = new Date(kstNow);
    todayStart.setUTCHours(0 - 9, 0, 0, 0);
    const todayEnd = new Date(kstNow);
    todayEnd.setUTCHours(23 - 9, 59, 59, 999);

    const colorMap = {
      '1': '라벤더', '2': '세이지(초록)', '3': '포도(보라)',
      '4': '플라밍고(분홍)', '5': '바나나(노랑)', '6': '귤(주황)',
      '7': '공작(청록)', '8': '흑연(회색)', '9': '블루베리(파랑)',
      '10': '바질(초록)', '11': '토마토(빨강)',
    };

    for (const event of events) {
      const start = new Date(event.start?.dateTime || event.start?.date);
      const end = new Date(event.end?.dateTime || event.end?.date);
      
      const colorId = event.colorId || '0';
      let eventType = 'other';
      if (colorId === '6') eventType = 'meeting';
      else if (colorId === '3') eventType = 'product';
      else if (['8', '9'].includes(colorId)) eventType = 'ops';
      else if (['2', '10'].includes(colorId)) eventType = 'growth';
      else if (['4', '5'].includes(colorId)) eventType = 'personal';
      
      const eventData = {
        id: event.id,
        title: event.summary || '제목 없음',
        start: start,
        end: end,
        startStr: event.start?.dateTime 
          ? start.toLocaleString('ko-KR', { timeZone: 'Asia/Seoul', month: 'numeric', day: 'numeric', hour: 'numeric', minute: '2-digit', hour12: true })
          : formatDateString(start),
        duration: Math.round((end - start) / (1000 * 60)),
        location: event.location || '',
        description: event.description || '',
        attendees: (event.attendees || []).map(a => ({
          email: a.email,
          name: a.displayName || a.email.split('@')[0],
          response: a.responseStatus,
        })),
        isAllDay: !event.start?.dateTime,
        meetLink: event.hangoutLink || '',
        colorId: colorId,
        colorName: colorMap[colorId] || '기본',
        eventType: eventType,
        // 외부/내부 미팅 구분
        isExternal: !!(event.location || event.hangoutLink || 
                      (event.description && /zoom|meet\.google|teams/i.test(event.description))),
        meetingType: event.location ? '외부' : 
                    (event.hangoutLink || (event.description && /zoom|meet\.google|teams/i.test(event.description))) ? '외부-화상' : '내부',
      };

      if (start < todayStart) {
        pastEvents.push(eventData);
      } else if (start >= todayStart && start <= todayEnd) {
        todayEvents.push(eventData);
      } else {
        upcomingEvents.push(eventData);
      }
    }

    const thisWeekEvents = [...todayEvents, ...upcomingEvents].filter(e => {
      const daysDiff = (e.start - kstNow) / (1000 * 60 * 60 * 24);
      return daysDiff <= 7;
    });

    const actualMeetingMinutes = thisWeekEvents
      .filter(e => !e.isAllDay && e.eventType === 'meeting')
      .reduce((sum, e) => sum + e.duration, 0);
    
    const totalScheduledMinutes = thisWeekEvents
      .filter(e => !e.isAllDay)
      .reduce((sum, e) => sum + e.duration, 0);

    const hoursByType = {
      meeting: Math.round(thisWeekEvents.filter(e => e.eventType === 'meeting' && !e.isAllDay).reduce((s, e) => s + e.duration, 0) / 60 * 10) / 10,
      product: Math.round(thisWeekEvents.filter(e => e.eventType === 'product' && !e.isAllDay).reduce((s, e) => s + e.duration, 0) / 60 * 10) / 10,
      ops: Math.round(thisWeekEvents.filter(e => e.eventType === 'ops' && !e.isAllDay).reduce((s, e) => s + e.duration, 0) / 60 * 10) / 10,
      growth: Math.round(thisWeekEvents.filter(e => e.eventType === 'growth' && !e.isAllDay).reduce((s, e) => s + e.duration, 0) / 60 * 10) / 10,
      personal: Math.round(thisWeekEvents.filter(e => e.eventType === 'personal' && !e.isAllDay).reduce((s, e) => s + e.duration, 0) / 60 * 10) / 10,
    };

    const freeSlots = calculateFreeSlots(todayEvents, upcomingEvents.slice(0, 20));

    log('INFO', 'Calendar', `캘린더: 오늘 ${todayEvents.length}건, 예정 ${upcomingEvents.length}건`);

    return {
      past: pastEvents,
      today: todayEvents,
      upcoming: upcomingEvents,
      thisWeek: thisWeekEvents,
      stats: {
        actualMeetingHours: Math.round(actualMeetingMinutes / 60 * 10) / 10,
        totalScheduledHours: Math.round(totalScheduledMinutes / 60 * 10) / 10,
        hoursByType,
        totalEventsThisWeek: thisWeekEvents.length,
      },
      freeSlots,
    };
  } catch (error) {
    log('ERROR', 'Calendar', `Google Calendar 가져오기 실패: ${error.message}`);
    return null;
  }
}

function calculateFreeSlots(todayEvents, upcomingEvents) {
  const slots = [];
  const workStart = 9;
  const workEnd = 18;
  
  const now = new Date();
  const currentHour = now.getHours();
  
  if (currentHour < workEnd) {
    const todayBusy = todayEvents
      .filter(e => !e.isAllDay)
      .map(e => ({
        start: e.start.getHours() + e.start.getMinutes() / 60,
        end: e.end.getHours() + e.end.getMinutes() / 60,
      }))
      .sort((a, b) => a.start - b.start);

    let freeStart = Math.max(currentHour, workStart);
    for (const busy of todayBusy) {
      if (busy.start > freeStart && busy.start < workEnd) {
        const duration = busy.start - freeStart;
        if (duration >= 1) {
          slots.push({
            date: '오늘',
            start: `${Math.floor(freeStart)}시`,
            duration: `${Math.round(duration)}시간`,
          });
        }
      }
      freeStart = Math.max(freeStart, busy.end);
    }
    
    if (freeStart < workEnd) {
      const duration = workEnd - freeStart;
      if (duration >= 1) {
        slots.push({
          date: '오늘',
          start: `${Math.floor(freeStart)}시`,
          duration: `${Math.round(duration)}시간`,
        });
      }
    }
  }

  return slots.slice(0, 5);
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
      
      const breakdown = {};
      for (const [category, colIdx] of Object.entries(categoryColumns)) {
        breakdown[category] = colIdx >= 0 ? parseNumber(row[colIdx]) : 0;
      }

      revenueData.push({
        date: dateStr,
        total,
        hasData,
        breakdown,
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
    const requiredDailyAvg = remainingDays > 0 
      ? Math.round((monthlyTarget - mtdRevenue) / remainingDays)
      : 0;
    
    const last7DaysAvg = validData.slice(0, 7).reduce((sum, d) => sum + d.total, 0) / Math.min(7, validData.length);
    const projectedMonthEnd = mtdRevenue + (last7DaysAvg * remainingDays);

    const stats = calculateRevenueStats(validData.slice(0, days));

    return {
      data: validData.slice(0, days),
      allData: revenueData,
      stats,
      sheetName,
      lastUpdated: latestValidData?.date || '알 수 없음',
      yesterdayStr,
      hasYesterdayData,
      yesterdayTotal: hasYesterdayData ? yesterdayData.total : null,
      monthlyAnalysis: {
        target: monthlyTarget,
        mtd: mtdRevenue,
        progress: parseFloat(targetProgress),
        remainingDays,
        requiredDailyAvg,
        last7DaysAvg: Math.round(last7DaysAvg),
        projectedMonthEnd: Math.round(projectedMonthEnd),
        onTrack: projectedMonthEnd >= monthlyTarget * 0.9,
      },
    };
  } catch (error) {
    log('ERROR', 'Revenue', `Google Sheets 매출 데이터 가져오기 실패: ${error.message}`);
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

  const trend = totals.length >= 3 
    ? (totals[0] + totals[1]) / 2 > (totals[totals.length - 2] + totals[totals.length - 1]) / 2
      ? 'up'
      : 'down'
    : 'stable';

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
    trend,
  };
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
          allMessages.push({
            channel: channel.name,
            user: msg.user,
            userName: userMap[msg.user] || '알 수 없음',
            text: msg.text,
            timestamp: msg.ts,
            isThread: false,
            replyCount: msg.reply_count || 0,
            threadTs: msg.thread_ts,
          });

          if (msg.thread_ts) {
            try {
              const replies = await slack.conversations.replies({
                channel: channel.id,
                ts: msg.thread_ts,
                limit: 200,
              });

              for (const reply of replies.messages.slice(1)) {
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
            } catch (err) {}
          }
        }

        await new Promise(resolve => setTimeout(resolve, 200));
      } catch (err) {}
    }

    log('INFO', 'Slack', `스레드 댓글 수집: ${threadCount}개`);
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
              } catch (err) {}
            }
          }
        }

        await new Promise(resolve => setTimeout(resolve, 100));
      } catch (err) {}
    }

    log('INFO', 'Slack', `DM 스레드 댓글 수집: ${threadCount}개`);
    return allDMs;
  } catch (error) {
    log('ERROR', 'Slack', `CEO DM 가져오기 실패: ${error.message}`);
    return [];
  }
}

// ============================================
// [NEW] Notion 깊은 탐색 - 핵심 개선 영역
// ============================================

// 수집 통계 (디버깅용)
const notionStats = {
  searchApiPages: 0,
  childPagesFound: 0,
  dbItemsWithContent: 0,
  blocksRead: 0,
  commentsRead: 0,
  maxDepthReached: 0,
  errors: [],
};

// 핵심 루트 페이지 ID (환경변수에서 가져옴)
function getRootPageIds() {
  const rootPages = process.env.NOTION_ROOT_PAGES || '';
  return rootPages.split(',').map(id => id.trim()).filter(Boolean);
}

// 블록 컨텐츠 추출 (강화 버전)
function extractTextFromBlockEnhanced(block, depth = 0) {
  const type = block.type;
  const content = block[type];
  const indent = '  '.repeat(depth);
  
  let text = '';
  if (content?.rich_text) {
    text = content.rich_text.map(t => t.plain_text).join('');
  }
  
  switch (type) {
    case 'heading_1': return `${indent}# ${text}`;
    case 'heading_2': return `${indent}## ${text}`;
    case 'heading_3': return `${indent}### ${text}`;
    case 'bulleted_list_item': return `${indent}• ${text}`;
    case 'numbered_list_item': return `${indent}1. ${text}`;
    case 'to_do': return `${indent}${content.checked ? '✓' : '○'} ${text}`;
    case 'toggle': return `${indent}▸ ${text}`;
    case 'quote': return `${indent}> ${text}`;
    case 'callout': 
      const emoji = content.icon?.emoji || '📌';
      return `${indent}${emoji} ${text}`;
    case 'code': 
      return `${indent}\`\`\`${content.language || ''}\n${text}\n\`\`\``;
    case 'divider': return `${indent}---`;
    case 'table_row':
      const cells = content.cells?.map(c => c.map(t => t.plain_text).join('')).join(' | ');
      return cells ? `${indent}| ${cells} |` : '';
    case 'child_page':
      return `${indent}📄 [하위 페이지: ${content.title}]`;
    case 'child_database':
      return `${indent}📊 [하위 데이터베이스: ${content.title}]`;
    case 'bookmark':
      return `${indent}🔗 ${content.url || ''}`;
    case 'embed':
      return `${indent}🔗 임베드: ${content.url || ''}`;
    case 'link_to_page':
      return `${indent}📎 링크된 페이지`;
    case 'synced_block':
      return ''; // 동기화 블록은 내용을 따로 가져와야 함
    case 'column_list':
    case 'column':
      return ''; // 컬럼은 하위 블록에서 처리
    default:
      return text ? `${indent}${text}` : '';
  }
}

// 블록 컨텐츠 재귀 수집 (페이지네이션 + 깊이 증가)
async function getBlockContentRecursive(blockId, maxDepth = 4, currentDepth = 0) {
  if (currentDepth >= maxDepth) {
    notionStats.maxDepthReached++;
    return '';
  }
  
  try {
    let allBlocks = [];
    let cursor = undefined;
    let pageCount = 0;
    
    // 페이지네이션으로 모든 블록 가져오기
    do {
      const response = await notion.blocks.children.list({
        block_id: blockId,
        page_size: 100,
        start_cursor: cursor,
      });
      
      allBlocks.push(...response.results);
      cursor = response.has_more ? response.next_cursor : undefined;
      pageCount++;
      notionStats.blocksRead += response.results.length;
      
      // 너무 많은 페이지 방지
      if (pageCount >= 5) break;
      
    } while (cursor);
    
    let content = '';
    
    for (const block of allBlocks) {
      const text = extractTextFromBlockEnhanced(block, currentDepth);
      if (text) {
        content += text + '\n';
      }
      
      // 하위 블록 있으면 재귀 (child_page, child_database는 별도 처리)
      if (block.has_children && 
          block.type !== 'child_page' && 
          block.type !== 'child_database') {
        const childContent = await getBlockContentRecursive(block.id, maxDepth, currentDepth + 1);
        content += childContent;
      }
    }

    return content;
  } catch (error) {
    notionStats.errors.push(`블록 ${blockId}: ${error.message}`);
    return '';
  }
}

// 페이지 댓글 수집 (블록 레벨 댓글 포함)
async function getPageComments(pageId) {
  const comments = [];
  
  try {
    // 페이지 레벨 댓글
    const pageComments = await notion.comments.list({ block_id: pageId });
    for (const comment of pageComments.results) {
      comments.push({
        type: 'page',
        author: comment.created_by?.id || 'unknown',
        text: comment.rich_text?.map(t => t.plain_text).join('') || '',
        createdAt: comment.created_time,
      });
      notionStats.commentsRead++;
    }
    
    // 블록 레벨 댓글 (상위 10개 블록만)
    const blocks = await notion.blocks.children.list({ block_id: pageId, page_size: 10 });
    for (const block of blocks.results) {
      try {
        const blockComments = await notion.comments.list({ block_id: block.id });
        for (const comment of blockComments.results) {
          const blockText = extractTextFromBlockEnhanced(block).slice(0, 50);
          comments.push({
            type: 'block',
            blockContext: blockText,
            author: comment.created_by?.id || 'unknown',
            text: comment.rich_text?.map(t => t.plain_text).join('') || '',
            createdAt: comment.created_time,
          });
          notionStats.commentsRead++;
        }
      } catch (err) {
        // 블록 댓글 접근 실패 (권한 등)
      }
    }
  } catch (error) {
    notionStats.errors.push(`댓글 ${pageId}: ${error.message}`);
  }
  
  return comments;
}

// 페이지 상세 정보 수집 (강화 버전)
async function getPageInfoDeepV2(page, includeContent = true) {
  try {
    let title = '제목 없음';
    if (page.properties) {
      const titleProp = Object.values(page.properties).find(prop => prop.type === 'title');
      if (titleProp?.title?.[0]) title = titleProp.title[0].plain_text;
    }
    
    // child_page 블록인 경우 제목 처리
    if (page.type === 'child_page' && page.child_page?.title) {
      title = page.child_page.title;
    }

    let content = '';
    if (includeContent) {
      content = await getBlockContentRecursive(page.id, 4); // depth 4
    }

    const comments = await getPageComments(page.id);

    // 페이지 경로 추출 시도
    let path = '';
    if (page.parent) {
      if (page.parent.type === 'page_id') {
        path = `상위 페이지: ${page.parent.page_id}`;
      } else if (page.parent.type === 'database_id') {
        path = `DB: ${page.parent.database_id}`;
      } else if (page.parent.type === 'workspace') {
        path = '워크스페이스 루트';
      }
    }

    return {
      id: page.id,
      title,
      content: content.slice(0, 2500), // 글자 수 증가
      lastEditedTime: page.last_edited_time,
      lastEditedBy: page.last_edited_by?.id || 'unknown',
      comments,
      url: page.url || `https://notion.so/${page.id.replace(/-/g, '')}`,
      path,
      depth: page.depth || 0,
      isDbItem: page.isDbItem || false,
    };
  } catch (error) {
    notionStats.errors.push(`페이지 ${page.id}: ${error.message}`);
    return null;
  }
}

// [NEW] 하위 페이지 재귀 탐색
async function getChildPagesRecursive(parentId, maxDepth = 4, currentDepth = 0, since = null) {
  if (currentDepth >= maxDepth) {
    log('DEBUG', 'Notion', `최대 깊이 도달: ${parentId} (depth ${currentDepth})`);
    return [];
  }
  
  const allPages = [];
  
  try {
    let cursor = undefined;
    let pageCount = 0;
    
    do {
      const blocks = await notion.blocks.children.list({
        block_id: parentId,
        page_size: 100,
        start_cursor: cursor,
      });
      
      for (const block of blocks.results) {
        // 하위 페이지 발견
        if (block.type === 'child_page') {
          notionStats.childPagesFound++;
          
          // 최근 수정 여부 확인 (since가 있는 경우)
          const isRecent = !since || new Date(block.last_edited_time) >= new Date(since);
          
          if (isRecent) {
            log('DEBUG', 'Notion', `하위 페이지 발견: ${block.child_page?.title} (depth ${currentDepth + 1})`);
            
            const pageInfo = await getPageInfoDeepV2({
              id: block.id,
              type: 'child_page',
              child_page: block.child_page,
              last_edited_time: block.last_edited_time,
              last_edited_by: block.last_edited_by,
              parent: { type: 'page_id', page_id: parentId },
              properties: {},
            });
            
            if (pageInfo) {
              pageInfo.depth = currentDepth + 1;
              pageInfo.parentId = parentId;
              allPages.push(pageInfo);
            }
          }
          
          // 재귀적으로 하위 탐색 (최근 수정 여부와 관계없이)
          const childPages = await getChildPagesRecursive(block.id, maxDepth, currentDepth + 1, since);
          allPages.push(...childPages);
        }
        
        // 하위 데이터베이스 발견
        if (block.type === 'child_database') {
          log('DEBUG', 'Notion', `하위 DB 발견: ${block.child_database?.title} (depth ${currentDepth + 1})`);
          
          const dbItems = await getDatabaseItemsWithContent(block.id, since);
          allPages.push(...dbItems);
        }
      }
      
      cursor = blocks.has_more ? blocks.next_cursor : undefined;
      pageCount++;
      
      if (pageCount >= 3) break; // 한 레벨에서 너무 많은 페이지 방지
      
    } while (cursor);
    
  } catch (error) {
    notionStats.errors.push(`하위 탐색 ${parentId}: ${error.message}`);
    log('WARN', 'Notion', `하위 페이지 탐색 실패 (${parentId}): ${error.message}`);
  }
  
  return allPages;
}

// [NEW] 데이터베이스 아이템 + 내부 컨텐츠
async function getDatabaseItemsWithContent(databaseId, since = null) {
  const itemsWithContent = [];
  
  try {
    const queryOptions = {
      database_id: databaseId,
      page_size: 20,
    };
    
    // 최근 수정된 것만 필터 (since가 있는 경우)
    if (since) {
      queryOptions.filter = {
        timestamp: 'last_edited_time',
        last_edited_time: { on_or_after: since },
      };
    }
    
    const items = await notion.databases.query(queryOptions);
    
    for (const item of items.results) {
      notionStats.dbItemsWithContent++;
      
      // 아이템 속성 추출
      const titleProp = Object.values(item.properties).find(p => p.type === 'title');
      const title = titleProp?.title?.[0]?.plain_text || '제목 없음';
      
      // 주요 속성 추출
      const properties = extractRelevantProperties(item.properties);
      
      // [핵심] 아이템 내부 컨텐츠 읽기
      const content = await getBlockContentRecursive(item.id, 3);
      
      // 댓글 수집
      const comments = await getPageComments(item.id);
      
      itemsWithContent.push({
        id: item.id,
        title,
        content: content.slice(0, 1500),
        lastEditedTime: item.last_edited_time,
        properties,
        comments,
        isDbItem: true,
        url: item.url || `https://notion.so/${item.id.replace(/-/g, '')}`,
      });
    }
    
    log('DEBUG', 'Notion', `DB ${databaseId}: ${itemsWithContent.length}개 아이템 (컨텐츠 포함)`);
    
  } catch (error) {
    notionStats.errors.push(`DB 아이템 ${databaseId}: ${error.message}`);
  }
  
  return itemsWithContent;
}

// 속성 추출 헬퍼
function extractRelevantProperties(properties) {
  const relevant = {};
  
  for (const [key, prop] of Object.entries(properties)) {
    switch (prop.type) {
      case 'status':
        if (prop.status?.name) relevant[key] = prop.status.name;
        break;
      case 'select':
        if (prop.select?.name) relevant[key] = prop.select.name;
        break;
      case 'multi_select':
        if (prop.multi_select?.length) relevant[key] = prop.multi_select.map(s => s.name).join(', ');
        break;
      case 'date':
        if (prop.date?.start) relevant[key] = prop.date.start;
        break;
      case 'people':
        if (prop.people?.length) relevant[key] = prop.people.map(p => p.name || p.id).join(', ');
        break;
      case 'checkbox':
        relevant[key] = prop.checkbox ? '✓' : '○';
        break;
      case 'number':
        if (prop.number !== null) relevant[key] = prop.number;
        break;
      case 'url':
        if (prop.url) relevant[key] = prop.url;
        break;
      case 'email':
        if (prop.email) relevant[key] = prop.email;
        break;
      case 'rich_text':
        if (prop.rich_text?.length) relevant[key] = prop.rich_text.map(t => t.plain_text).join('');
        break;
    }
  }
  
  return relevant;
}

// [NEW] 메인 Notion 수집 함수 (통합) - 성능 최적화 버전
async function getRecentNotionPagesDeep(days = 1) {
  const startTime = Date.now();
  const MAX_EXECUTION_TIME = 60000; // 60초 제한 (안전 마진)
  
  // 통계 초기화
  Object.assign(notionStats, {
    searchApiPages: 0,
    childPagesFound: 0,
    dbItemsWithContent: 0,
    blocksRead: 0,
    commentsRead: 0,
    maxDepthReached: 0,
    errors: [],
  });
  
  const allPages = [];
  const since = new Date(Date.now() - (86400000 * days)).toISOString();
  const seenIds = new Set();
  
  // 시간 체크 헬퍼
  const isTimeUp = () => (Date.now() - startTime) > MAX_EXECUTION_TIME;
  
  log('INFO', 'Notion', `Notion 수집 시작 (since: ${since})`);
  
  // 1. Search API로 최근 수정된 페이지 가져오기 (제목만, 빠르게)
  try {
    const searchResults = await notion.search({
      filter: { property: 'object', value: 'page' },
      sort: { direction: 'descending', timestamp: 'last_edited_time' },
      page_size: 50, // 50개로 제한
    });
    
    const recentFromSearch = searchResults.results.filter(p => p.last_edited_time >= since);
    notionStats.searchApiPages = recentFromSearch.length;
    
    log('INFO', 'Notion', `Search API: ${recentFromSearch.length}개 페이지 (최근 ${days}일)`);
    
    // 상위 20개만 처리, 병렬로 (5개씩 배치)
    const pagesToProcess = recentFromSearch.slice(0, 20);
    const batchSize = 5;
    
    for (let i = 0; i < pagesToProcess.length && !isTimeUp(); i += batchSize) {
      const batch = pagesToProcess.slice(i, i + batchSize);
      
      const results = await Promise.all(
        batch.map(async (page) => {
          if (seenIds.has(page.id)) return null;
          seenIds.add(page.id);
          
          // 상위 10개만 컨텐츠 포함, 나머지는 제목만
          const includeContent = i < 10;
          const pageInfo = await getPageInfoLite(page, includeContent);
          if (pageInfo) {
            pageInfo.source = 'search_api';
          }
          return pageInfo;
        })
      );
      
      allPages.push(...results.filter(Boolean));
      log('DEBUG', 'Notion', `Search API 배치 ${i / batchSize + 1} 완료 (${Date.now() - startTime}ms)`);
    }
  } catch (error) {
    log('ERROR', 'Notion', `Search API 실패: ${error.message}`);
  }
  
  if (isTimeUp()) {
    log('WARN', 'Notion', '시간 제한 도달 - Search API만으로 완료');
    return finalizeResults(allPages);
  }
  
  // 2. 루트 페이지에서 하위 탐색 (depth 2로 제한, 빠르게)
  const rootPageIds = getRootPageIds();
  
  if (rootPageIds.length > 0) {
    log('INFO', 'Notion', `루트 페이지 탐색 시작: ${rootPageIds.length}개`);
    
    for (const rootId of rootPageIds) {
      if (isTimeUp()) {
        log('WARN', 'Notion', '시간 제한 도달 - 루트 탐색 중단');
        break;
      }
      
      log('DEBUG', 'Notion', `루트 페이지 탐색: ${rootId}`);
      
      try {
        // depth 2로 제한, 컨텐츠 없이 제목만
        const childPages = await getChildPagesLite(rootId, 2, 0, since, seenIds);
        
        for (const page of childPages) {
          if (!seenIds.has(page.id)) {
            seenIds.add(page.id);
            page.source = 'recursive_search';
            allPages.push(page);
          }
        }
        
        log('DEBUG', 'Notion', `루트 ${rootId.slice(0, 8)}...: ${childPages.length}개 하위 페이지 (${Date.now() - startTime}ms)`);
      } catch (error) {
        log('WARN', 'Notion', `루트 ${rootId.slice(0, 8)}... 탐색 실패: ${error.message}`);
      }
    }
  }
  
  if (isTimeUp()) {
    log('WARN', 'Notion', '시간 제한 도달 - DB 탐색 스킵');
    return finalizeResults(allPages);
  }
  
  // 3. 최근 수정된 DB 아이템 (상위 5개 DB만)
  try {
    const dbSearch = await notion.search({
      filter: { property: 'object', value: 'database' },
      page_size: 10,
    });
    
    // 최근 수정된 DB만 필터
    const recentDbs = dbSearch.results
      .filter(db => db.last_edited_time >= since)
      .slice(0, 5);
    
    log('DEBUG', 'Notion', `최근 수정된 DB: ${recentDbs.length}개`);
    
    for (const db of recentDbs) {
      if (isTimeUp()) break;
      
      const dbItems = await getDatabaseItemsLite(db.id, since, 5); // 5개로 제한
      
      for (const item of dbItems) {
        if (!seenIds.has(item.id)) {
          seenIds.add(item.id);
          item.source = 'database_query';
          item.databaseName = db.title?.[0]?.plain_text || 'Unknown DB';
          allPages.push(item);
        }
      }
    }
  } catch (error) {
    log('WARN', 'Notion', `데이터베이스 탐색 실패: ${error.message}`);
  }
  
  return finalizeResults(allPages);
  
  // 결과 정리 헬퍼
  function finalizeResults(pages) {
    const uniquePages = Array.from(
      new Map(pages.map(p => [p.id, p])).values()
    );
    
    uniquePages.sort((a, b) => new Date(b.lastEditedTime) - new Date(a.lastEditedTime));
    
    const elapsed = Date.now() - startTime;
    
    // 수집 통계 로깅
    log('INFO', 'Notion', '=== Notion 수집 통계 ===');
    log('INFO', 'Notion', `소요 시간: ${elapsed}ms`);
    log('INFO', 'Notion', `Search API 페이지: ${notionStats.searchApiPages}개`);
    log('INFO', 'Notion', `하위 페이지 발견: ${notionStats.childPagesFound}개`);
    log('INFO', 'Notion', `DB 아이템: ${notionStats.dbItemsWithContent}개`);
    log('INFO', 'Notion', `총 블록 읽음: ${notionStats.blocksRead}개`);
    log('INFO', 'Notion', `최종 페이지 수: ${uniquePages.length}개`);
    
    if (notionStats.errors.length > 0) {
      log('WARN', 'Notion', `오류 ${notionStats.errors.length}건`);
    }
    
    return {
      pages: uniquePages.slice(0, 40),
      stats: { ...notionStats },
    };
  }
}

// [NEW] 라이트 버전 - 페이지 정보 (컨텐츠 선택적)
async function getPageInfoLite(page, includeContent = false) {
  try {
    let title = '제목 없음';
    if (page.properties) {
      const titleProp = Object.values(page.properties).find(prop => prop.type === 'title');
      if (titleProp?.title?.[0]) title = titleProp.title[0].plain_text;
    }
    
    if (page.type === 'child_page' && page.child_page?.title) {
      title = page.child_page.title;
    }

    let content = '';
    if (includeContent) {
      content = await getBlockContentRecursive(page.id, 2); // depth 2로 제한
    }

    return {
      id: page.id,
      title,
      content: content.slice(0, 1000),
      lastEditedTime: page.last_edited_time,
      lastEditedBy: page.last_edited_by?.id || 'unknown',
      url: page.url || `https://notion.so/${page.id.replace(/-/g, '')}`,
      hasFullContent: includeContent,
    };
  } catch (error) {
    notionStats.errors.push(`페이지 ${page.id}: ${error.message}`);
    return null;
  }
}

// [NEW] 라이트 버전 - 하위 페이지 탐색 (since 필터 제거, 컨텐츠 일부 포함)
async function getChildPagesLite(parentId, maxDepth = 2, currentDepth = 0, since = null, seenIds = new Set()) {
  if (currentDepth >= maxDepth) {
    return [];
  }
  
  const allPages = [];
  
  try {
    const blocks = await notion.blocks.children.list({
      block_id: parentId,
      page_size: 50,
    });
    
    for (const block of blocks.results) {
      if (block.type === 'child_page') {
        notionStats.childPagesFound++;
        
        // since 필터 완전 제거 - 모든 하위 페이지 포함
        if (!seenIds.has(block.id)) {
          seenIds.add(block.id);
          
          // 상위 3개 페이지는 컨텐츠도 읽기
          let content = '';
          if (allPages.length < 3) {
            try {
              content = await getBlockContentRecursive(block.id, 2);
            } catch (e) {
              // 실패해도 제목은 포함
            }
          }
          
          allPages.push({
            id: block.id,
            title: block.child_page?.title || '제목 없음',
            content: content.slice(0, 600),
            lastEditedTime: block.last_edited_time,
            depth: currentDepth + 1,
            parentId: parentId,
            hasFullContent: content.length > 0,
          });
        }
        
        // depth 2까지 재귀
        if (currentDepth + 1 < maxDepth) {
          const childPages = await getChildPagesLite(block.id, maxDepth, currentDepth + 1, since, seenIds);
          allPages.push(...childPages);
        }
      }
      
      // 하위 데이터베이스도 수집 (최근 아이템 3개)
      if (block.type === 'child_database' && allPages.length < 15) {
        const dbTitle = block.child_database?.title || 'DB';
        log('DEBUG', 'Notion', `하위 DB 발견: ${dbTitle} (depth ${currentDepth + 1})`);
        
        try {
          const dbItems = await getDatabaseItemsLite(block.id, null, 3);
          for (const item of dbItems) {
            if (!seenIds.has(item.id)) {
              seenIds.add(item.id);
              allPages.push({
                ...item,
                depth: currentDepth + 1,
                parentId: parentId,
                databaseName: dbTitle,
              });
            }
          }
        } catch (e) {
          // DB 접근 실패 무시
        }
      }
    }
    
  } catch (error) {
    notionStats.errors.push(`하위 탐색 ${parentId}: ${error.message}`);
  }
  
  return allPages;
}

// [NEW] 라이트 버전 - DB 아이템 (제한된 수, 컨텐츠 없이)
async function getDatabaseItemsLite(databaseId, since = null, limit = 5) {
  const items = [];
  
  try {
    const queryOptions = {
      database_id: databaseId,
      page_size: limit,
      sorts: [{ timestamp: 'last_edited_time', direction: 'descending' }],
    };
    
    if (since) {
      queryOptions.filter = {
        timestamp: 'last_edited_time',
        last_edited_time: { on_or_after: since },
      };
    }
    
    const result = await notion.databases.query(queryOptions);
    
    for (const item of result.results) {
      notionStats.dbItemsWithContent++;
      
      const titleProp = Object.values(item.properties).find(p => p.type === 'title');
      const title = titleProp?.title?.[0]?.plain_text || '제목 없음';
      
      const properties = extractRelevantProperties(item.properties);
      
      items.push({
        id: item.id,
        title,
        content: '', // 컨텐츠 없이
        lastEditedTime: item.last_edited_time,
        properties,
        isDbItem: true,
        hasFullContent: false,
      });
    }
    
    log('DEBUG', 'Notion', `DB ${databaseId.slice(0, 8)}...: ${items.length}개 아이템`);
    
  } catch (error) {
    notionStats.errors.push(`DB 아이템 ${databaseId}: ${error.message}`);
  }
  
  return items;
}

// Notion 사용자 목록
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
// Claude 분석
// ============================================
async function analyzeWithClaude(slackMessages, ceoDMs, notionData, revenueData, calendarData, days = 1) {
  const { pages, stats: notionStats } = notionData;
  const users = await getNotionUsers();

  // Slack 포맷팅
  let slackSection = '메시지 없음';
  if (slackMessages.length > 0) {
    const sorted = [...slackMessages].sort((a, b) => parseFloat(a.timestamp) - parseFloat(b.timestamp));
    slackSection = sorted.map(m => {
      const threadTag = m.isThread ? '  ↳ [스레드]' : '';
      const replyInfo = m.replyCount > 0 ? ` (답글 ${m.replyCount}개)` : '';
      return `${threadTag}[${m.channel}] ${m.userName}: ${m.text}${replyInfo}`;
    }).join('\n');
  }

  // DM 포맷팅
  let dmSection = 'DM 없음';
  if (ceoDMs.length > 0) {
    const sorted = [...ceoDMs].sort((a, b) => parseFloat(a.timestamp) - parseFloat(b.timestamp));
    dmSection = sorted.map(m => {
      const threadTag = m.isThread ? '  ↳ [스레드]' : '';
      const replyInfo = m.replyCount > 0 ? ` (답글 ${m.replyCount}개)` : '';
      return `${threadTag}[${m.channel}] ${m.userName}: ${m.text}${replyInfo}`;
    }).join('\n');
  }

  // [NEW] Notion 포맷팅 (깊이 정보 포함)
  let notionPagesSection = '업데이트된 페이지 없음';
  if (pages.length > 0) {
    notionPagesSection = pages.map(p => {
      const editor = users[p.lastEditedBy] || '알 수 없음';
      const depthIndicator = p.depth ? `(depth ${p.depth})` : '';
      const sourceIndicator = p.source ? `[${p.source}]` : '';
      const dbIndicator = p.isDbItem ? `[DB: ${p.databaseName || 'DB아이템'}]` : '';
      
      let section = `📄 [${p.title}] ${depthIndicator} ${sourceIndicator} ${dbIndicator}`;
      section += `\n   수정: ${editor} | ${p.lastEditedTime}`;
      
      if (p.properties && Object.keys(p.properties).length > 0) {
        section += `\n   속성: ${JSON.stringify(p.properties)}`;
      }
      
      if (p.content) {
        section += `\n   내용:\n${p.content.split('\n').map(line => '   ' + line).join('\n').slice(0, 800)}`;
      }
      
      if (p.comments && p.comments.length > 0) {
        section += `\n   💬 댓글 (${p.comments.length}개):`;
        p.comments.slice(0, 3).forEach(c => {
          const author = users[c.author] || '익명';
          const context = c.blockContext ? ` (블록: "${c.blockContext}...")` : '';
          section += `\n      - ${author}${context}: ${c.text}`;
        });
      }
      
      return section;
    }).join('\n\n');
  }

  // 매출 데이터 포맷팅
  let revenueSection = '매출 데이터 없음';
  if (revenueData?.data?.length > 0) {
    const stats = revenueData.stats;
    const ma = revenueData.monthlyAnalysis;
    const recentDays = revenueData.data.slice(0, 7);
    
    let yesterdayInfo = revenueData.hasYesterdayData
      ? `어제(${revenueData.yesterdayStr}) 매출: ${formatWon(revenueData.yesterdayTotal)}`
      : `⚠ 어제(${revenueData.yesterdayStr}) 데이터 없음\n가장 최근 데이터: ${stats.latestDate} - ${formatWon(stats.latestTotal)}`;
    
    const diff = stats.dayOverDayDiff;
    const diffSign = diff >= 0 ? '+' : '';
    
    revenueSection = `[매출 현황 - ${revenueData.sheetName} 시트]

${yesterdayInfo}
전일(${stats.previousDate}) 매출: ${formatWon(stats.previousTotal)}
전일 대비: ${diffSign}${formatWon(Math.abs(diff))} (${stats.dayOverDayChange > 0 ? '+' : ''}${stats.dayOverDayChange}%)
7일 평균: ${formatWon(stats.avg7Day)}

[월간 목표 대비 분석]
월 목표: ${formatWon(ma.target)}
MTD 매출: ${formatWon(ma.mtd)} (목표의 ${ma.progress}%)
잔여 일수: ${ma.remainingDays}일
목표 달성 필요 일평균: ${formatWon(ma.requiredDailyAvg)}
최근 7일 평균: ${formatWon(ma.last7DaysAvg)}
예상 월말 매출: ${formatWon(ma.projectedMonthEnd)} (${ma.onTrack ? '목표 달성 가능' : '⚠ 목표 미달 예상'})

최근 데이터 수익원 Top 5:
${stats.topCategories.map(([cat, val]) => `  - ${cat}: ${formatWon(val)}`).join('\n')}

최근 7일 매출:
${recentDays.map(d => `  ${d.date}: ${formatWon(d.total)}`).join('\n')}`;
  }

  // 캘린더 데이터 포맷팅
  let calendarSection = '캘린더 데이터 없음';
  if (calendarData && calendarData.today) {
    // 외부/내부 미팅 카운트
    const externalCount = calendarData.today.filter(e => e.isExternal).length;
    const internalCount = calendarData.today.filter(e => !e.isExternal && !e.isAllDay).length;
    
    const todayList = calendarData.today.length > 0
      ? calendarData.today.map(e => {
          const typeTag = e.eventType === 'meeting' ? '🟠' :
                         e.eventType === 'product' ? '🟣' :
                         e.eventType === 'ops' ? '🔵' :
                         e.eventType === 'growth' ? '🟢' :
                         e.eventType === 'personal' ? '🟡' : '⚪';
          const meetingTypeTag = e.meetingType ? `[${e.meetingType}]` : '';
          const locationInfo = e.location ? ` 📍${e.location}` : '';
          const meetLinkInfo = e.meetLink ? ' 🔗화상' : '';
          return `  ${typeTag} ${e.startStr}: ${e.title} ${meetingTypeTag} (${e.duration}분)${locationInfo}${meetLinkInfo}${e.attendees.length > 0 ? ` [${e.attendees.map(a => a.name).join(', ')}]` : ''}`;
        }).join('\n')
      : '  (일정 없음)';
    
    const upcomingList = calendarData.upcoming.slice(0, 10).map(e => {
      const typeTag = e.eventType === 'meeting' ? '🟠' :
                     e.eventType === 'product' ? '🟣' :
                     e.eventType === 'ops' ? '🔵' :
                     e.eventType === 'growth' ? '🟢' :
                     e.eventType === 'personal' ? '🟡' : '⚪';
      const meetingTypeTag = e.meetingType ? `[${e.meetingType}]` : '';
      return `  ${typeTag} ${e.startStr}: ${e.title} ${meetingTypeTag}${e.attendees.length > 0 ? ` [${e.attendees.map(a => a.name).join(', ')}]` : ''}`;
    }).join('\n');

    const freeSlotsList = calendarData.freeSlots.length > 0
      ? calendarData.freeSlots.map(s => `  - ${s.date} ${s.start}부터 ${s.duration}`).join('\n')
      : '  (빈 시간 없음)';

    const hbt = calendarData.stats?.hoursByType || {};
    
    calendarSection = `[오늘 일정] (🟠미팅 🟣프로덕트 🔵운영 🟢자기계발 🟡여가)
총 ${calendarData.today.length}건 (외부 ${externalCount}건 / 내부 ${internalCount}건)
${todayList}

[이번 주 시간 배분]
- 🟠 실제 미팅: ${hbt.meeting || 0}시간
- 🟣 프로덕트: ${hbt.product || 0}시간
- 🔵 운영업무: ${hbt.ops || 0}시간
- 🟢 자기계발: ${hbt.growth || 0}시간
- 🟡 여가: ${hbt.personal || 0}시간
- 전체: ${calendarData.stats?.totalScheduledHours || 0}시간

[향후 주요 일정]
${upcomingList}

[집중 가능 시간대]
${freeSlotsList}`;
  }

  // Claude 프롬프트
  const prompt = `당신은 월 2~3억 매출의 Web3 스타트업 CEO의 Chief of Staff입니다.
CEO가 아침에 읽고 바로 의사결정하고 행동할 수 있는 브리핑을 작성합니다.

[CEO 컨텍스트]
- 최근 구조조정 완료 (23명 → 17명), 조직 안정화 중
- 교보생명 PoC 데드라인 (1월 13일) 중요
- 2026년 목표: MAU 300K, 월 광고매출 3-4억, Q4 흑자전환
- 성향: 직접적/합리적 피드백 선호, 데이터 기반 의사결정
- 비기술 창업자로 AI 자동화에 적극적

[핵심 원칙]
1. 목표 대비 현재 위치를 명확히 - 숫자로 Gap 표시
2. 모든 이슈에 오너십(누가)과 데드라인(언제까지) 명시
3. 의사결정이 필요하면 옵션과 추천안 제시
4. CEO 시간 배분 가이드 제공 (구체적 시간/퍼센트)
5. 스레드 맥락 파악 - 결론 난 건 [해결됨] 표시
6. 캘린더 데이터가 있으면 반드시 오늘 일정과 미팅 브리프에 포함할 것
7. Notion 페이지의 깊이(depth)와 출처(source)를 참고하여 중요도 판단

═══════════════════════════════════
[CEO 캘린더]
═══════════════════════════════════
${calendarSection}

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
[Notion 페이지 업데이트] (수집 통계: Search API ${notionStats.searchApiPages}개, 하위페이지 ${notionStats.childPagesFound}개, DB아이템 ${notionStats.dbItemsWithContent}개)
═══════════════════════════════════
${notionPagesSection}

═══════════════════════════════════

아래 형식으로 브리핑을 작성하세요. 볼드(**) 사용하지 마세요.

# CEO 대시보드

> 💡 [한 줄 코칭: CEO의 현재 상황(구조조정 직후, 연말, 2026 준비)을 고려한 실질적 조언 한 문장]

## 1) 핵심 지표 현황
매출:
- 어제: [금액] | 전일대비: [%] | 7일평균대비: [%]
- 월 목표 대비: MTD [금액] ([%])
- 목표 달성 전망: [달성 가능/⚠ 미달 예상 - 근거]

오늘 일정: [N]건 (외부 [N]건 / 내부 [N]건)
집중 가능 시간: [시간대]

## 2) 의사결정 필요 (우선순위순)

### 🔴 이슈명
배경: 1줄
옵션:
  A) [선택지1] → 예상 결과
  B) [선택지2] → 예상 결과
추천: [A/B] - [근거 1줄]
담당: [이름] | 결정 기한: [날짜]

### 🟡 이슈명
(동일 형식)

### 🟢 이슈명
(동일 형식)

(의사결정 필요 없으면 "오늘 결정할 사항 없음")

## 3) 실행 추적

### 즉시 (오늘)
- [ ] [할일] → [담당] | [시간/기한]

### 단기 (이번주)
- [ ] [할일] → [담당] | [요일]까지

### 중기 (2주)
- [ ] [할일] → [담당] | [날짜]까지

## 4) 금주 CEO 시간 배분 권장

| 영역 | 배분 | 시간 | 구체적 행동 |
|------|------|------|------------|
| [영역1] | [N]% | [N]시간 | [무엇을 어떻게] |
| [영역2] | [N]% | [N]시간 | [무엇을 어떻게] |
| [영역3] | [N]% | [N]시간 | [무엇을 어떻게] |
| [영역4] | [N]% | [N]시간 | [무엇을 어떻게] |

(주 40시간 기준으로 계산)

이번 주 하지 말 것: [에너지 쏟을 필요 없는 것들 - 구체적으로]

## 5) 리스크 모니터링

[🟢/🟡/🔴] 영역명
- 현황: 1줄
- 주시 포인트: 무엇을 지켜봐야 하는지

## 6) 오늘의 미팅 브리프

[시간] 미팅명 [외부/내부/외부-화상]
- 참석자: [누구와]
- 목적/아젠다: 
- 준비 필요: 
- 원하는 결과:

[외부/내부 구분 기준]
- 장소(location)가 있으면 → [외부]
- Google Meet/Zoom 링크가 있으면 → [외부-화상]
- 둘 다 없으면 → [내부]

---
[주의사항]
- 숫자는 정확하게, 불확실하면 "⚠ 확인 필요"
- 담당자/기한 없는 액션 아이템 금지
- 볼드(**) 사용 금지
- 의사결정 우선순위는 반드시 🔴🟡🟢 이모지로 표시
- 시간 배분은 주 40시간 기준으로 시간까지 계산해서 제공
- 한 줄 코칭은 CEO의 현재 상황과 컨텍스트를 반영한 실질적 조언으로`;

  try {
    const message = await anthropic.messages.create({
      model: 'claude-sonnet-4-20250514',
      max_tokens: 3500,
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
    const today = new Date();
    const dateStr = `${today.getMonth() + 1}/${today.getDate()}`;
    const dayNames = ['일', '월', '화', '수', '목', '금', '토'];
    const dayName = dayNames[today.getDay()];
    const headerText = `📊 CEO 대시보드 (${dateStr} ${dayName})`;
    
    let statsText = `Slack ${stats.slackCount} | DM ${stats.dmCount} | Notion ${stats.notionPages}`;
    statsText += ` (Search ${stats.notionStats?.searchApiPages || 0} + Child ${stats.notionStats?.childPagesFound || 0} + DB ${stats.notionStats?.dbItemsWithContent || 0})`;
    
    if (stats.revenueDataAvailable) {
      statsText += ` | 매출 ${stats.hasYesterdayData ? '✓' : '(어제 없음)'}`;
    }
    if (stats.calendarAvailable) {
      statsText += ` | 캘린더 ✓`;
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
        { type: 'divider' },
        {
          type: 'context',
          elements: [{
            type: 'mrkdwn',
            text: `${new Date().toLocaleString('ko-KR')} | Claude Sonnet 4 | Notion Deep Scan v2`,
          }],
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
  log('INFO', 'Main', `CEO 대시보드 생성 시작 (v2 - Deep Notion Scan)`);
  log('INFO', 'Main', `분석 기간: ${days}일`);
  log('INFO', 'Main', `현재 시각 (KST): ${getKSTDate().toISOString()}`);
  console.log('='.repeat(60));

  try {
    // 0. 캘린더 데이터 수집
    log('INFO', 'Main', '캘린더 데이터 수집 중...');
    const calendarData = await getCalendarEvents(days, 7);

    // 1. 매출 데이터 수집
    log('INFO', 'Main', '매출 데이터 수집 중...');
    const revenueData = await getRevenueData(Math.max(days, 7));

    // 2. Slack 메시지 수집
    log('INFO', 'Main', 'Slack 메시지 수집 중...');
    const { messages: slackMessages, userMap } = await getSlackMessages(days);

    // 3. CEO DM 수집
    log('INFO', 'Main', 'CEO DM 수집 중...');
    const ceoDMs = await getCEODirectMessages(userMap, days);

    // 4. [NEW] Notion 깊은 수집
    log('INFO', 'Main', 'Notion 깊은 수집 중...');
    const notionData = await getRecentNotionPagesDeep(days);

    // 5. Claude 분석
    log('INFO', 'Main', 'Claude 분석 중...');
    const analysis = await analyzeWithClaude(
      slackMessages, 
      ceoDMs, 
      notionData,
      revenueData,
      calendarData,
      days
    );

    // 6. CEO에게 발송
    log('INFO', 'Main', 'CEO에게 DM 발송 중...');
    await sendDMToCEO(analysis, {
      slackCount: slackMessages.length,
      dmCount: ceoDMs.length,
      notionPages: notionData.pages.length,
      notionStats: notionData.stats,
      days,
      revenueDataAvailable: !!revenueData,
      hasYesterdayData: revenueData?.hasYesterdayData || false,
      calendarAvailable: !!calendarData,
    });

    log('INFO', 'Main', '완료!');

    res.status(200).json({
      success: true,
      days,
      stats: {
        slackMessages: slackMessages.length,
        ceoDMs: ceoDMs.length,
        notion: {
          totalPages: notionData.pages.length,
          searchApiPages: notionData.stats.searchApiPages,
          childPagesFound: notionData.stats.childPagesFound,
          dbItemsWithContent: notionData.stats.dbItemsWithContent,
          blocksRead: notionData.stats.blocksRead,
          commentsRead: notionData.stats.commentsRead,
          errors: notionData.stats.errors.length,
        },
        calendar: calendarData ? {
          today: calendarData.today.length,
          upcoming: calendarData.upcoming.length,
        } : null,
        revenue: revenueData ? {
          days: revenueData.data.length,
          hasYesterdayData: revenueData.hasYesterdayData,
        } : null,
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
