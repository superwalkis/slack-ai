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
    
    // 전체 데이터 범위 가져오기 (A~AA, 충분히 넓게)
    const range = `${sheetName}!A:AA`;
    
    const response = await sheets.spreadsheets.values.get({
      spreadsheetId,
      range,
    });

    const rows = response.data.values;
    if (!rows || rows.length < 3) {
      console.log('매출 데이터 없음');
      return null;
    }

    // 헤더 행 (2번째 행, 인덱스 1)
    const headers = rows[1];
    
    // 날짜 컬럼 인덱스 찾기 (AA열 = 26번째, 0-indexed = 26)
    const dateColIndex = headers.findIndex(h => h && h.includes('날짜')) !== -1 
      ? headers.findIndex(h => h && h.includes('날짜'))
      : 26; // AA열
    
    // 합계 컬럼 인덱스 찾기
    const totalColIndex = headers.findIndex(h => h && h.includes('합계'));
    
    // 주요 컬럼 인덱스
    const colIndexes = {
      수수료: headers.findIndex(h => h && h.includes('수수료')),
      이벤트상점: headers.findIndex(h => h && h.includes('이벤트')),
      특가상품: headers.findIndex(h => h && h.includes('특가')),
      자동수리패스: headers.findIndex(h => h && h.includes('자동수리')),
      자동컴플패스: headers.findIndex(h => h && h.includes('자동컴플')),
      광고네트워크: headers.findIndex(h => h && h.includes('네트워크')),
      광고직판: headers.findIndex(h => h && h.includes('직판')),
      이커머스: headers.findIndex(h => h && h.includes('E-커머스') || h && h.includes('커머스')),
    };

    // 데이터 행 파싱 (3번째 행부터)
    const revenueData = [];
    
    for (let i = 2; i < rows.length; i++) {
      const row = rows[i];
      if (!row || row.length < 10) continue;
      
      // 날짜 파싱
      const dateStr = row[dateColIndex];
      if (!dateStr || dateStr === '-') continue;
      
      // 합계 파싱 (₩ 기호와 쉼표 제거)
      const totalStr = row[totalColIndex];
      if (!totalStr || totalStr === '-' || totalStr === '₩') continue;
      
      const total = parseNumber(totalStr);
      if (total === 0) continue;

      const dayData = {
        date: dateStr,
        total: total,
        breakdown: {
          수수료: parseNumber(row[colIndexes.수수료]),
          이벤트상점: parseNumber(row[colIndexes.이벤트상점]),
          특가상품: parseNumber(row[colIndexes.특가상품]),
          자동수리패스: parseNumber(row[colIndexes.자동수리패스]),
          자동컴플패스: parseNumber(row[colIndexes.자동컴플패스]),
          광고네트워크: parseNumber(row[colIndexes.광고네트워크]),
          광고직판: parseNumber(row[colIndexes.광고직판]),
          이커머스: parseNumber(row[colIndexes.이커머스]),
        }
      };
      
      revenueData.push(dayData);
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
  const latest = totals[0];
  const previous = totals[1] || latest;
  const avg7Day = totals.reduce((sum, t) => sum + t, 0) / totals.length;

  // 카테고리별 합계
  const categoryTotals = {};
  const categories = ['수수료', '이벤트상점', '특가상품', '자동수리패스', '자동컴플패스', '광고네트워크', '광고직판', '이커머스'];
  
  categories.forEach(cat => {
    categoryTotals[cat] = data.reduce((sum, d) => sum + (d.breakdown[cat] || 0), 0);
  });

  // 가장 큰 수익원 찾기
  const topCategory = Object.entries(categoryTotals)
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
    categoryTotals,
  };
}

function formatRevenue(num) {
  if (num >= 100000000) {
    return `${(num / 100000000).toFixed(1)}억`;
  } else if (num >= 10000) {
    return `${(num / 10000).toFixed(0)}만`;
  }
  return num.toLocaleString();
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

    // 페이지 콘텐츠 가져오기
    const blocks = await notion.blocks.children.list({
      block_id: page.id,
      page_size: 50,
    });

    const content = extractTextFromBlocks(blocks.results);

    // 댓글 가져오기
    const comments = await getPageComments(page.id);

    return {
      id: page.id,
      title,
      content: content.slice(0, 2000),
      comments,
      lastEditedTime: page.last_edited_time,
      lastEditedBy: page.last_edited_by?.id || '알 수 없음',
      url: page.url,
    };
  } catch (error) {
    return null;
  }
}

function extractTextFromBlocks(blocks) {
  let text = '';

  for (const block of blocks) {
    const blockType = block.type;
    const blockContent = block[blockType];

    if (blockContent?.rich_text) {
      const blockText = blockContent.rich_text
        .map(t => t.plain_text)
        .join('');
      text += blockText + '\n';
    }

    if (blockType === 'to_do' && blockContent) {
      const checked = blockContent.checked ? '✅' : '⬜';
      text += `${checked} `;
    }
  }

  return text.trim();
}

async function getPageComments(pageId) {
  try {
    const response = await notion.comments.list({
      block_id: pageId,
    });

    return response.results.map(comment => ({
      author: comment.created_by?.id || '알 수 없음',
      text: comment.rich_text?.map(t => t.plain_text).join('') || '',
      createdTime: comment.created_time,
    }));
  } catch (error) {
    return [];
  }
}

async function getNotionDatabases(days = 1) {
  try {
    const since = new Date(Date.now() - (86400000 * days)).toISOString();

    const response = await notion.search({
      filter: {
        property: 'object',
        value: 'database',
      },
    });

    const databaseSummaries = [];

    for (const db of response.results.slice(0, 5)) {
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
              after: since,
            },
          },
          page_size: 20,
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

  // ✅ 매출 데이터 포맷팅
  let revenueSection = '매출 데이터 없음 (시트 미연동 또는 데이터 없음)';
  if (revenueData && revenueData.data && revenueData.data.length > 0) {
    const stats = revenueData.stats;
    const recentDays = revenueData.data.slice(0, 7);
    
    revenueSection = `📊 매출 현황 (${revenueData.sheetName} 시트, 최종 업데이트: ${revenueData.lastUpdated})

💰 최근 매출:
${recentDays.map(d => `  ${d.date}: ₩${formatRevenue(d.total)}`).join('\n')}

📈 통계:
  - 최근 일 매출: ₩${formatRevenue(stats.latestTotal)}
  - 전일 대비: ${stats.dayOverDayChange > 0 ? '+' : ''}${stats.dayOverDayChange}%
  - ${stats.daysCount}일 평균: ₩${formatRevenue(stats.avg7Day)}
  - 평균 대비: ${stats.avgChange > 0 ? '+' : ''}${stats.avgChange}%
  - 기간 총 매출: ₩${formatRevenue(stats.totalPeriod)}

🏆 Top 수익원:
${stats.topCategories.map(([cat, val]) => `  - ${cat}: ₩${formatRevenue(val)}`).join('\n')}`;
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
