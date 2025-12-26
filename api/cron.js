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
// Google Calendar 일정 수집 (NEW)
// ============================================
async function getCalendarEvents(daysBack = 1, daysForward = 7) {
  try {
    const credentials = JSON.parse(process.env.GOOGLE_SERVICE_ACCOUNT_JSON || '{}');
    
    if (!credentials.client_email) {
      console.log('Google 서비스 계정 미설정 - 캘린더 스킵');
      return null;
    }

    // CEO 이메일 (환경변수로 설정 필요)
    const ceoEmail = process.env.CEO_GOOGLE_EMAIL;
    if (!ceoEmail) {
      console.log('CEO_GOOGLE_EMAIL 미설정 - 캘린더 스킵');
      return null;
    }

    const auth = new google.auth.GoogleAuth({
      credentials,
      scopes: ['https://www.googleapis.com/auth/calendar.readonly'],
    });

    // 도메인 전체 위임 사용 - 서비스 계정이 CEO 대신 행동
    const authClient = await auth.getClient();
    authClient.subject = ceoEmail;

    const calendar = google.calendar({ version: 'v3', auth: authClient });

    const now = new Date();
    const timeMin = new Date(now.getTime() - (daysBack * 24 * 60 * 60 * 1000));
    const timeMax = new Date(now.getTime() + (daysForward * 24 * 60 * 60 * 1000));

    const response = await calendar.events.list({
      calendarId: ceoEmail, // 또는 'primary'
      timeMin: timeMin.toISOString(),
      timeMax: timeMax.toISOString(),
      singleEvents: true,
      orderBy: 'startTime',
      maxResults: 50,
    });

    const events = response.data.items || [];
    
    // 이벤트 분류
    const pastEvents = [];
    const todayEvents = [];
    const upcomingEvents = [];
    
    const todayStart = new Date();
    todayStart.setHours(0, 0, 0, 0);
    const todayEnd = new Date();
    todayEnd.setHours(23, 59, 59, 999);

    for (const event of events) {
      const start = new Date(event.start?.dateTime || event.start?.date);
      const end = new Date(event.end?.dateTime || event.end?.date);
      
      // Google Calendar 색상 ID 매핑
      // 1: 라벤더, 2: 세이지, 3: 포도, 4: 플라밍고, 5: 바나나
      // 6: 귤, 7: 공작, 8: 흑연, 9: 블루베리, 10: 바질, 11: 토마토
      const colorMap = {
        '1': '라벤더',
        '2': '세이지(초록)',
        '3': '포도(보라)',
        '4': '플라밍고(분홍)',
        '5': '바나나(노랑)',
        '6': '귤(주황)',
        '7': '공작(청록)',
        '8': '흑연(회색)',
        '9': '블루베리(파랑)',
        '10': '바질(초록)',
        '11': '토마토(빨강)',
      };
      
      // Tim 캘린더 색상 분류
      // 주황 = 실제 미팅
      // 보라 = 프로덕트 관련 업무 (기획/리서치)
      // 파랑/회색 = 개인 업무 (운영/HR/경영지원/연락)
      // 초록 = 자기계발
      // 노랑/분홍 = 노는 시간
      const colorId = event.colorId || '0';
      let eventType = 'other';
      if (colorId === '6') eventType = 'meeting';           // 주황 = 실제 미팅
      else if (colorId === '3') eventType = 'product';      // 보라 = 프로덕트
      else if (['8', '9'].includes(colorId)) eventType = 'ops';  // 회색/파랑 = 개인업무(운영)
      else if (['2', '10'].includes(colorId)) eventType = 'growth';  // 초록 = 자기계발
      else if (['4', '5'].includes(colorId)) eventType = 'personal'; // 분홍/노랑 = 노는시간
      
      const eventData = {
        id: event.id,
        title: event.summary || '제목 없음',
        start: start,
        end: end,
        startStr: event.start?.dateTime 
          ? start.toLocaleString('ko-KR', { month: 'numeric', day: 'numeric', hour: 'numeric', minute: '2-digit' })
          : formatDateString(start),
        duration: Math.round((end - start) / (1000 * 60)), // 분 단위
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
        eventType: eventType,  // meeting, work, personal, other
      };

      if (start < todayStart) {
        pastEvents.push(eventData);
      } else if (start >= todayStart && start <= todayEnd) {
        todayEvents.push(eventData);
      } else {
        upcomingEvents.push(eventData);
      }
    }

    // 이번 주 시간 분석
    const thisWeekEvents = [...todayEvents, ...upcomingEvents].filter(e => {
      const daysDiff = (e.start - now) / (1000 * 60 * 60 * 24);
      return daysDiff <= 7;
    });

    // 실제 미팅 시간만 계산 (주황색 = meeting)
    const actualMeetingMinutes = thisWeekEvents
      .filter(e => !e.isAllDay && e.eventType === 'meeting')
      .reduce((sum, e) => sum + e.duration, 0);
    
    const totalScheduledMinutes = thisWeekEvents
      .filter(e => !e.isAllDay)
      .reduce((sum, e) => sum + e.duration, 0);

    const actualMeetingHours = Math.round(actualMeetingMinutes / 60 * 10) / 10;
    const totalScheduledHours = Math.round(totalScheduledMinutes / 60 * 10) / 10;
    
    // 색상 기반 카테고리 (eventType 사용)
    const byEventType = {
      '실제미팅(주황)': thisWeekEvents.filter(e => e.eventType === 'meeting').length,
      '프로덕트(보라)': thisWeekEvents.filter(e => e.eventType === 'product').length,
      '운영업무(파랑/회색)': thisWeekEvents.filter(e => e.eventType === 'ops').length,
      '자기계발(초록)': thisWeekEvents.filter(e => e.eventType === 'growth').length,
      '여가(노랑/분홍)': thisWeekEvents.filter(e => e.eventType === 'personal').length,
    };
    
    // 시간 계산 (분 → 시간)
    const hoursByType = {
      meeting: Math.round(thisWeekEvents.filter(e => e.eventType === 'meeting' && !e.isAllDay).reduce((s, e) => s + e.duration, 0) / 60 * 10) / 10,
      product: Math.round(thisWeekEvents.filter(e => e.eventType === 'product' && !e.isAllDay).reduce((s, e) => s + e.duration, 0) / 60 * 10) / 10,
      ops: Math.round(thisWeekEvents.filter(e => e.eventType === 'ops' && !e.isAllDay).reduce((s, e) => s + e.duration, 0) / 60 * 10) / 10,
      growth: Math.round(thisWeekEvents.filter(e => e.eventType === 'growth' && !e.isAllDay).reduce((s, e) => s + e.duration, 0) / 60 * 10) / 10,
      personal: Math.round(thisWeekEvents.filter(e => e.eventType === 'personal' && !e.isAllDay).reduce((s, e) => s + e.duration, 0) / 60 * 10) / 10,
    };
    
    // 키워드 기반 카테고리 (기존)
    const categories = {
      '1:1': thisWeekEvents.filter(e => /1:1|1on1|면담/.test(e.title)).length,
      '팀미팅': thisWeekEvents.filter(e => /팀|스탠드업|싱크|sync|standup/.test(e.title.toLowerCase())).length,
      '외부미팅': thisWeekEvents.filter(e => e.attendees.some(a => !a.email.includes(process.env.COMPANY_DOMAIN || ''))).length,
      '집중시간': thisWeekEvents.filter(e => /집중|focus|블록|block/.test(e.title.toLowerCase())).length,
    };

    // 빈 시간대 분석 (오전 9시-오후 6시 기준)
    const freeSlots = calculateFreeSlots(todayEvents, upcomingEvents.slice(0, 20));

    console.log(`📅 캘린더: 과거 ${pastEvents.length}개, 오늘 ${todayEvents.length}개, 예정 ${upcomingEvents.length}개`);
    console.log(`   실제 미팅(주황): ${byEventType['실제미팅(주황)']}건, ${actualMeetingHours}시간`);

    return {
      past: pastEvents,
      today: todayEvents,
      upcoming: upcomingEvents,
      thisWeek: thisWeekEvents,
      stats: {
        actualMeetingHours,      // 실제 미팅만 (주황)
        totalScheduledHours,     // 전체 일정
        categories,              // 키워드 기반
        byEventType,             // 색상 기반 (건수)
        hoursByType,             // 색상 기반 (시간)
        totalEventsThisWeek: thisWeekEvents.length,
      },
      freeSlots,
    };
  } catch (error) {
    console.error('Google Calendar 가져오기 실패:', error.message);
    return null;
  }
}

function calculateFreeSlots(todayEvents, upcomingEvents) {
  const slots = [];
  const workStart = 9; // 오전 9시
  const workEnd = 18; // 오후 6시
  
  // 오늘 남은 빈 시간
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
        if (duration >= 1) { // 1시간 이상만
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

  return slots.slice(0, 5); // 상위 5개만
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
    
    const kstNow = getKSTDate();
    const sheetName = `${String(kstNow.getFullYear()).slice(2)}.${String(kstNow.getMonth() + 1).padStart(2, '0')}`;
    
    console.log(`📊 시트 이름: ${sheetName}`);
    
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
    
    // 날짜/합계 컬럼 찾기
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

    // 카테고리 컬럼 찾기
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
    
    // 월간 목표 대비 분석 (환경변수로 설정 가능)
    const monthlyTarget = parseInt(process.env.MONTHLY_REVENUE_TARGET) || 200_000_000; // 기본 2억
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
      // 목표 대비 분석
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

  // 7일 트렌드 (상승/하락)
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
// Slack 메시지 수집 (스레드 강화)
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

    console.log(`📧 스레드 댓글 수집: ${threadCount}개`);
    return { messages: allMessages, userMap };
  } catch (error) {
    console.error('Slack 메시지 가져오기 실패:', error);
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

    console.log(`💬 DM 스레드 댓글 수집: ${threadCount}개`);
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
      filter: { property: 'object', value: 'page' },
      sort: { direction: 'descending', timestamp: 'last_edited_time' },
      page_size: 100,
    });

    const recentPages = response.results.filter(page => page.last_edited_time >= since);
    const pagesWithContent = [];

    for (const page of recentPages.slice(0, 30)) {
      try {
        const pageInfo = await getPageInfoDeep(page);
        if (pageInfo) pagesWithContent.push(pageInfo);
      } catch (err) {}
    }

    return pagesWithContent;
  } catch (error) {
    console.error('Notion 페이지 가져오기 실패:', error);
    return [];
  }
}

async function getPageInfoDeep(page) {
  try {
    let title = '제목 없음';
    if (page.properties) {
      const titleProp = Object.values(page.properties).find(prop => prop.type === 'title');
      if (titleProp?.title?.[0]) title = titleProp.title[0].plain_text;
    }

    const content = await getBlockContentRecursive(page.id, 2);

    let comments = [];
    try {
      const commentsResponse = await notion.comments.list({ block_id: page.id });
      comments = commentsResponse.results.map(comment => ({
        author: comment.created_by?.id || 'unknown',
        text: comment.rich_text?.map(t => t.plain_text).join('') || '',
        createdAt: comment.created_time,
      }));
    } catch (err) {}

    return {
      id: page.id,
      title,
      content: content.slice(0, 1500),
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
    const blocks = await notion.blocks.children.list({ block_id: blockId, page_size: 50 });
    let content = '';
    
    for (const block of blocks.results) {
      const text = extractTextFromBlock(block);
      if (text) {
        const indent = '  '.repeat(currentDepth);
        content += `${indent}${text}\n`;
      }
      
      if (block.has_children) {
        content += await getBlockContentRecursive(block.id, maxDepth, currentDepth + 1);
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
  if (!content?.rich_text) return '';
  
  const text = content.rich_text.map(t => t.plain_text).join('');
  
  switch (type) {
    case 'heading_1': return `# ${text}`;
    case 'heading_2': return `## ${text}`;
    case 'heading_3': return `### ${text}`;
    case 'bulleted_list_item': return `• ${text}`;
    case 'numbered_list_item': return `- ${text}`;
    case 'to_do': return `${content.checked ? '✓' : '○'} ${text}`;
    case 'toggle': return `▸ ${text}`;
    default: return text;
  }
}

async function getNotionDatabases(days = 1) {
  try {
    const since = new Date(Date.now() - (86400000 * days)).toISOString();
    
    const response = await notion.search({
      filter: { property: 'object', value: 'database' },
      page_size: 30,
    });

    const databaseSummaries = [];

    for (const db of response.results) {
      try {
        let dbTitle = db.title?.[0]?.plain_text || '제목 없음';

        const items = await notion.databases.query({
          database_id: db.id,
          filter: {
            timestamp: 'last_edited_time',
            last_edited_time: { on_or_after: since },
          },
          page_size: 20,
        });

        if (items.results.length > 0) {
          const itemSummaries = items.results.map(item => {
            const titleProp = Object.values(item.properties).find(p => p.type === 'title');
            const title = titleProp?.title?.[0]?.plain_text || '제목 없음';
            const statusProp = Object.values(item.properties).find(p => p.type === 'status' || p.type === 'select');
            const status = statusProp?.status?.name || statusProp?.select?.name || '';
            const dateProp = Object.values(item.properties).find(p => p.type === 'date');
            const date = dateProp?.date?.start || '';

            return { title, status, date, lastEdited: item.last_edited_time };
          });

          databaseSummaries.push({
            name: dbTitle,
            recentItems: itemSummaries,
            totalUpdated: items.results.length,
          });
        }
      } catch (err) {}
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
// Claude 분석 (의사결정 지원 시스템)
// ============================================
async function analyzeWithClaude(slackMessages, ceoDMs, notionData, revenueData, calendarData, days = 1) {
  const { pages, databases, users } = notionData;

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

  // Notion 포맷팅
  let notionPagesSection = '업데이트된 페이지 없음';
  if (pages.length > 0) {
    notionPagesSection = pages.map(p => {
      const editor = users[p.lastEditedBy] || '알 수 없음';
      let section = `[${p.title}] (수정: ${editor})\n내용: ${p.content.slice(0, 500)}`;
      if (p.comments.length > 0) {
        section += `\n댓글 (${p.comments.length}개):\n`;
        section += p.comments.map(c => `  - ${users[c.author] || '익명'}: ${c.text}`).join('\n');
      }
      return section;
    }).join('\n\n');
  }

  let notionDbSection = '업데이트된 데이터베이스 없음';
  if (databases.length > 0) {
    notionDbSection = databases.map(db => {
      const items = db.recentItems.map(item => 
        `  - ${item.title}${item.status ? ` [${item.status}]` : ''}${item.date ? ` (${item.date})` : ''}`
      ).join('\n');
      return `[${db.name}] (${db.totalUpdated}개 업데이트)\n${items}`;
    }).join('\n\n');
  }

  // 매출 데이터 포맷팅 (목표 대비 분석 포함)
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
  console.log('📅 캘린더 데이터 확인:', calendarData ? `있음 (오늘 ${calendarData.today?.length}건)` : '없음');
  
  if (calendarData && calendarData.today) {
    const todayList = calendarData.today.length > 0
      ? calendarData.today.map(e => {
          const typeTag = e.eventType === 'meeting' ? '🟠' :   // 주황 = 미팅
                         e.eventType === 'product' ? '🟣' :    // 보라 = 프로덕트
                         e.eventType === 'ops' ? '🔵' :        // 파랑/회색 = 운영
                         e.eventType === 'growth' ? '🟢' :     // 초록 = 자기계발
                         e.eventType === 'personal' ? '🟡' :   // 노랑/분홍 = 여가
                         '⚪';
          return `  ${typeTag} ${e.startStr}: ${e.title} (${e.duration}분)${e.attendees.length > 0 ? ` [${e.attendees.map(a => a.name).join(', ')}]` : ''}`;
        }).join('\n')
      : '  (일정 없음)';
    
    const upcomingList = calendarData.upcoming.slice(0, 10).map(e => {
      const typeTag = e.eventType === 'meeting' ? '🟠' :
                     e.eventType === 'product' ? '🟣' :
                     e.eventType === 'ops' ? '🔵' :
                     e.eventType === 'growth' ? '🟢' :
                     e.eventType === 'personal' ? '🟡' :
                     '⚪';
      return `  ${typeTag} ${e.startStr}: ${e.title}${e.attendees.length > 0 ? ` [${e.attendees.map(a => a.name).join(', ')}]` : ''}`;
    }).join('\n');

    const freeSlotsList = calendarData.freeSlots.length > 0
      ? calendarData.freeSlots.map(s => `  - ${s.date} ${s.start}부터 ${s.duration}`).join('\n')
      : '  (빈 시간 없음)';

    const hbt = calendarData.stats?.hoursByType || { meeting: 0, product: 0, ops: 0, growth: 0, personal: 0 };
    
    calendarSection = `[오늘 일정] (🟠미팅 🟣프로덕트 🔵운영 🟢자기계발 🟡여가)
${todayList}

[이번 주 시간 배분]
- 🟠 실제 미팅: ${hbt.meeting}시간
- 🟣 프로덕트(기획/리서치): ${hbt.product}시간
- 🔵 운영업무(HR/경영지원): ${hbt.ops}시간
- 🟢 자기계발: ${hbt.growth}시간
- 🟡 여가: ${hbt.personal}시간
- 전체: ${calendarData.stats?.totalScheduledHours || 0}시간

[향후 주요 일정]
${upcomingList}

[집중 가능 시간대]
${freeSlotsList}`;

    console.log('📅 캘린더 섹션 생성 완료:', calendarSection.slice(0, 200) + '...');
  } else {
    console.log('📅 캘린더 데이터 없음 - calendarData:', !!calendarData, 'today:', !!calendarData?.today);
  }

  // ============================================
  // 의사결정 지원 프롬프트
  // ============================================
  const prompt = `당신은 월 2~3억 매출의 Web3 스타트업 CEO의 Chief of Staff입니다.
CEO가 아침에 읽고 바로 의사결정하고 행동할 수 있는 브리핑을 작성합니다.

[핵심 원칙]
1. 목표 대비 현재 위치를 명확히 - 숫자로 Gap 표시
2. 모든 이슈에 오너십(누가)과 데드라인(언제까지) 명시
3. 의사결정이 필요하면 옵션과 추천안 제시
4. CEO 시간 배분 가이드 제공
5. 스레드 맥락 파악 - 결론 난 건 [해결됨] 표시

═══════════════════════════════════
[매출 데이터]
═══════════════════════════════════
${revenueSection}

═══════════════════════════════════
[CEO 캘린더]
═══════════════════════════════════
${calendarSection}

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

# CEO 대시보드

## 1) 핵심 지표 현황
매출:
- 어제: [금액] | 전일대비: [%] | 7일평균대비: [%]
- 월 목표 ${revenueData?.monthlyAnalysis ? formatWon(revenueData.monthlyAnalysis.target) : '미설정'} 대비: MTD [금액] ([%])
- 목표 달성 전망: [달성 가능/⚠ 미달 예상 - 근거]

오늘 일정: [N]건, 미팅 시간 [N]시간 (캘린더 데이터가 있으면 반드시 건수와 시간 표시)
집중 가능 시간: [시간대] (캘린더 데이터가 있으면 반드시 표시)

## 2) 의사결정 필요 (우선순위순)

### [높음] 이슈명
배경: 1줄
옵션:
  A) [선택지1] → 예상 결과
  B) [선택지2] → 예상 결과
추천: [A/B] - [근거 1줄]
담당: [이름] | 결정 기한: [날짜]

(의사결정 필요 없으면 "오늘 결정할 사항 없음")

## 3) 실행 추적

### 즉시 (오늘)
- [ ] [할일] → [담당] | [시간/기한]
- [ ] [할일] → [담당] | [시간/기한]

### 단기 (이번주)
- [ ] [할일] → [담당] | [요일]까지

### 중기 (2주)
- [ ] [할일] → [담당] | [날짜]까지

(해당 없으면 항목 생략)

## 4) 금주 CEO 시간 배분 권장

1. [주제1] ([%]) - [이유]
2. [주제2] ([%]) - [이유]
3. [주제3] ([%]) - [이유]

이번 주 하지 말 것: [에너지 쏟을 필요 없는 것들]

## 5) 리스크 모니터링

[🟢/🟡/🔴] 영역명
- 현황: 1줄
- 주시 포인트: 무엇을 지켜봐야 하는지

(리스크 없으면 "주요 리스크 없음")

## 6) 오늘의 미팅 브리프
(캘린더 데이터가 있으면 반드시 아래 형식으로 각 미팅 정리. 없으면 "오늘 미팅 없음"만 표시)

[시간] 미팅명 (🟠/🟣/🔵/🟢/🟡 색상 표시)
- 목적/아젠다: 
- 준비 필요: 
- 원하는 결과:

---
[주의사항]
- 숫자는 정확하게, 불확실하면 "⚠ 확인 필요"
- 담당자/기한 없는 액션 아이템 금지
- 볼드(**) 사용 금지
- 이모지는 최소한으로`;

  try {
    const message = await anthropic.messages.create({
      model: 'claude-sonnet-4-20250514',
      max_tokens: 3500,
      messages: [{ role: 'user', content: prompt }],
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
    const dayNames = ['일', '월', '화', '수', '목', '금', '토'];
    const dayName = dayNames[today.getDay()];
    const headerText = `📊 CEO 대시보드 (${dateStr} ${dayName})`;
    
    let statsText = `Slack ${stats.slackCount} | DM ${stats.dmCount} | Notion ${stats.notionPages} | 스레드 ${stats.threadCount}`;
    if (stats.revenueDataAvailable) {
      statsText += ` | 매출 ${stats.hasYesterdayData ? '✓' : '(어제 데이터 없음)'}`;
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
            text: `${new Date().toLocaleString('ko-KR')} | Claude Sonnet 4`,
          }],
        },
      ],
    });

    // 긴 메시지 분할 발송
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
  console.log(`📊 CEO 대시보드 생성 시작`);
  console.log(`📆 분석 기간: ${days}일`);
  console.log(`📅 현재 시각 (KST): ${getKSTDate().toISOString()}`);
  console.log('='.repeat(50));

  try {
    // 0. 캘린더 데이터 수집
    console.log('\n📅 캘린더 데이터 수집 중...');
    const calendarData = await getCalendarEvents(days, 7);
    if (calendarData) {
      console.log(`✅ 캘린더: 오늘 ${calendarData.today.length}건, 예정 ${calendarData.upcoming.length}건`);
    }

    // 1. 매출 데이터 수집
    console.log('\n💰 매출 데이터 수집 중...');
    const revenueData = await getRevenueData(Math.max(days, 7));
    if (revenueData) {
      console.log(`✅ 매출 데이터: ${revenueData.data.length}일치`);
      console.log(`   월 목표 대비: ${revenueData.monthlyAnalysis.progress}%`);
    }

    // 2. Slack 메시지 수집
    console.log('\n📱 Slack 메시지 수집 중...');
    const { messages: slackMessages, userMap } = await getSlackMessages(days);
    const slackThreadCount = slackMessages.filter(m => m.isThread).length;
    console.log(`✅ Slack: ${slackMessages.length}개 (스레드 ${slackThreadCount}개)`);

    // 3. CEO DM 수집
    console.log('\n💬 CEO DM 수집 중...');
    const ceoDMs = await getCEODirectMessages(userMap, days);
    const dmThreadCount = ceoDMs.filter(m => m.isThread).length;
    console.log(`✅ CEO DM: ${ceoDMs.length}개 (스레드 ${dmThreadCount}개)`);

    // 4. Notion 데이터 수집
    console.log('\n👥 Notion 사용자 목록...');
    const notionUsers = await getNotionUsers();

    console.log('\n📝 Notion 페이지 수집 중...');
    const notionPages = await getRecentNotionPages(days);
    console.log(`✅ Notion 페이지: ${notionPages.length}개`);

    console.log('\n📊 Notion 데이터베이스 수집 중...');
    const notionDatabases = await getNotionDatabases(days);
    console.log(`✅ Notion DB: ${notionDatabases.length}개`);

    // 5. Claude 분석
    console.log('\n🤖 Claude 분석 중...');
    const analysis = await analyzeWithClaude(
      slackMessages, 
      ceoDMs, 
      { pages: notionPages, databases: notionDatabases, users: notionUsers },
      revenueData,
      calendarData,
      days
    );
    console.log('✅ 분석 완료');

    // 6. CEO에게 발송
    console.log('\n📤 CEO에게 DM 발송 중...');
    await sendDMToCEO(analysis, {
      slackCount: slackMessages.length,
      dmCount: ceoDMs.length,
      notionPages: notionPages.length,
      notionDbs: notionDatabases.length,
      days,
      revenueDataAvailable: !!revenueData,
      hasYesterdayData: revenueData?.hasYesterdayData || false,
      threadCount: slackThreadCount + dmThreadCount,
      calendarAvailable: !!calendarData,
      todayMeetings: calendarData?.today.length || 0,
    });

    console.log('\n✅ 완료!');

    res.status(200).json({
      success: true,
      days,
      stats: {
        slackMessages: slackMessages.length,
        slackThreads: slackThreadCount,
        ceoDMs: ceoDMs.length,
        dmThreads: dmThreadCount,
        notionPages: notionPages.length,
        notionDatabases: notionDatabases.length,
        calendar: calendarData ? {
          today: calendarData.today.length,
          upcoming: calendarData.upcoming.length,
          meetingHours: calendarData.stats.meetingHoursThisWeek,
        } : null,
        revenueData: revenueData ? {
          days: revenueData.data.length,
          latestTotal: revenueData.stats?.latestTotal,
          latestDate: revenueData.stats?.latestDate,
          hasYesterdayData: revenueData.hasYesterdayData,
          monthlyProgress: revenueData.monthlyAnalysis?.progress,
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
