const Anthropic = require('@anthropic-ai/sdk');
const { WebClient } = require('@slack/web-api');
const { Client } = require('@notionhq/client');

// ============================================
// 클라이언트 초기화
// ============================================
const anthropic = new Anthropic({
  apiKey: process.env.ANTHROPIC_API_KEY,
});

const slack = new WebClient(process.env.SLACK_BOT_TOKEN);

// CEO의 DM 접근용 User Token (Bot Token과 별도)
const slackUser = new WebClient(process.env.SLACK_USER_TOKEN);

const notion = new Client({
  auth: process.env.NOTION_API_KEY,
});

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

    let allMessages = [];

    for (const channel of channelsResult.channels) {
      try {
        const history = await slack.conversations.history({
          channel: channel.id,
          oldest: oldest,
          latest: now,
          limit: 200,
        });

        const messagesWithContext = history.messages.map(msg => ({
          channel: channel.name,
          user: msg.user,
          text: msg.text,
          timestamp: msg.ts,
        }));

        allMessages = allMessages.concat(messagesWithContext);
      } catch (err) {
        console.log(`채널 ${channel.name} 접근 불가:`, err.message);
      }
    }

    // 사용자 이름 매핑
    const usersResult = await slack.users.list();
    const userMap = {};
    usersResult.members.forEach(user => {
      userMap[user.id] = user.real_name || user.name;
    });

    allMessages = allMessages.map(msg => ({
      ...msg,
      userName: userMap[msg.user] || '알 수 없음',
    }));

    return { messages: allMessages, userMap };
  } catch (error) {
    console.error('Slack 메시지 가져오기 실패:', error);
    return { messages: [], userMap: {} };
  }
}

// ============================================
// CEO DM 수집 (User Token 필요)
// ============================================
async function getCEODirectMessages(userMap, days = 1) {
  try {
    const now = Math.floor(Date.now() / 1000);
    const oldest = now - (86400 * days);

    // CEO의 모든 DM 채널 가져오기
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
          // 상대방 이름 가져오기
          const otherUserId = dm.user;
          const otherUserName = userMap[otherUserId] || '알 수 없음';

          const dmMessages = history.messages.map(msg => ({
            channel: `DM:${otherUserName}`,
            user: msg.user,
            userName: userMap[msg.user] || '알 수 없음',
            text: msg.text,
            timestamp: msg.ts,
            isDM: true,
          }));

          allDMs = allDMs.concat(dmMessages);
        }
      } catch (err) {
        // DM 접근 실패는 조용히 넘어감
      }
    }

    console.log(`✅ CEO DM: ${allDMs.length}개 메시지`);
    return allDMs;
  } catch (error) {
    console.error('CEO DM 가져오기 실패:', error);
    console.log('💡 SLACK_USER_TOKEN이 설정되어 있는지 확인하세요.');
    return [];
  }
}

// ============================================
// Notion 데이터 수집
// ============================================

// 최근 수정된 페이지 가져오기
async function getRecentNotionPages() {
  try {
    const yesterday = new Date(Date.now() - 86400000).toISOString();
    
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

    // 최근 24시간 내 수정된 페이지만 필터링
    const recentPages = response.results.filter(page => {
      return page.last_edited_time >= yesterday;
    });

    const pagesWithContent = [];

    for (const page of recentPages.slice(0, 20)) { // 최대 20개
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

// 페이지 상세 정보 추출
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

    // 수정자 정보
    const lastEditedBy = page.last_edited_by?.id || '알 수 없음';

    return {
      id: page.id,
      title,
      content: content.slice(0, 2000), // 최대 2000자
      comments,
      lastEditedTime: page.last_edited_time,
      lastEditedBy,
      url: page.url,
    };
  } catch (error) {
    console.error(`페이지 정보 추출 실패:`, error);
    return null;
  }
}

// 블록에서 텍스트 추출
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

    // 할 일 목록 처리
    if (blockType === 'to_do' && blockContent) {
      const checked = blockContent.checked ? '✅' : '⬜';
      text += `${checked} `;
    }

    // 제목 처리
    if (blockType.startsWith('heading')) {
      text += '\n';
    }
  }

  return text.trim();
}

// 페이지 댓글 가져오기
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
    // 댓글 권한이 없을 수 있음
    return [];
  }
}

// 특정 데이터베이스 쿼리 (프로젝트/태스크 추적용)
async function getNotionDatabases() {
  try {
    const yesterday = new Date(Date.now() - 86400000).toISOString();

    const response = await notion.search({
      filter: {
        property: 'object',
        value: 'database',
      },
    });

    const databaseSummaries = [];

    for (const db of response.results.slice(0, 5)) { // 최대 5개 DB
      try {
        // 데이터베이스 제목 추출
        let dbTitle = '제목 없음';
        if (db.title && db.title[0]) {
          dbTitle = db.title[0].plain_text;
        }

        // 최근 수정된 항목 쿼리
        const items = await notion.databases.query({
          database_id: db.id,
          filter: {
            timestamp: 'last_edited_time',
            last_edited_time: {
              after: yesterday,
            },
          },
          page_size: 20,
        });

        if (items.results.length > 0) {
          const itemSummaries = items.results.map(item => {
            // 첫 번째 title 속성 찾기
            const titleProp = Object.values(item.properties).find(
              p => p.type === 'title'
            );
            const title = titleProp?.title?.[0]?.plain_text || '제목 없음';

            // status 속성 찾기
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

// Notion 사용자 이름 매핑
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
async function analyzeWithClaude(slackMessages, ceoDMs, notionData, days = 1) {
  const { pages, databases, users } = notionData;
  const isInitialRun = days > 1;

  // Slack 채널 메시지 포맷팅
  let slackSection = '메시지 없음';
  if (slackMessages.length > 0) {
    slackSection = slackMessages
      .map(m => `[${m.channel}] ${m.userName}: ${m.text}`)
      .join('\n');
  }

  // CEO DM 포맷팅
  let dmSection = 'DM 없음';
  if (ceoDMs.length > 0) {
    dmSection = ceoDMs
      .map(m => `[${m.channel}] ${m.userName}: ${m.text}`)
      .join('\n');
  }

  // Notion 페이지 포맷팅
  let notionPagesSection = '업데이트된 페이지 없음';
  if (pages.length > 0) {
    notionPagesSection = pages
      .map(p => {
        const editor = users[p.lastEditedBy] || '알 수 없음';
        let section = `📄 ${p.title} (수정: ${editor})\n내용 요약: ${p.content.slice(0, 500)}`;
        if (p.comments.length > 0) {
          section += `\n댓글: ${p.comments.map(c => 
            `${users[c.author] || '익명'}: ${c.text}`
          ).join(' | ')}`;
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

  // 초기 분석용 vs 일일 분석용 프롬프트
  const analysisFormat = isInitialRun ? `
다음 형식으로 ${days}일간의 종합 분석을 해주세요:

👥 팀원별 커뮤니케이션 패턴
   - 각 팀원과의 DM 빈도 및 주요 논의 주제
   - 소통이 잘 되는 팀원 vs 관심 필요한 팀원
   - 1:1 미팅 우선순위 추천

🔥 주요 이슈 타임라인
   - 기간 내 반복적으로 등장한 문제들
   - 해결된 이슈 vs 아직 열린 이슈
   - 에스컬레이션 필요한 사항

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
다음 형식으로 통합 분석해주세요:

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

🟢 칭찬할 점 / 좋은 진행상황
   - 팀원 이름
   - 기여 내용
   - 추천 액션

⚠️ 패턴 감지
   - 반복되는 문제
   - 소통 단절 징후
   - DM에서만 나온 이슈 (채널 공유 필요?)
   - 방향성 혼란

📊 생산성 인사이트
   - 가장 활발한 팀원/채널
   - 정체된 프로젝트
   - 1:1 미팅 필요해 보이는 팀원`;

  const prompt = `당신은 CEO의 Staff로서 조직을 모니터링합니다.
${isInitialRun ? `\n🚀 이것은 최초 분석입니다. 지난 ${days}일간의 데이터를 종합적으로 분석해주세요.\n` : ''}
═══════════════════════════════════
📱 Slack 채널 대화 (${days}일)
═══════════════════════════════════
${slackSection}

═══════════════════════════════════
💬 CEO 1:1 DM 대화 (${days}일)
═══════════════════════════════════
${dmSection}

═══════════════════════════════════
📝 Notion 업데이트
═══════════════════════════════════
[최근 수정된 페이지]
${notionPagesSection}

[데이터베이스 변경사항]
${notionDbSection}

═══════════════════════════════════
${analysisFormat}

분석 시 주의사항:
- DM 내용은 민감할 수 있으니 팩트 중심으로
- 채널 대화와 DM 교차 분석 (공개 vs 비공개 논의 갭)
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
    
    const statsText = `📈 수집 통계 (${stats.days}일): Slack 채널 ${stats.slackCount}개 | CEO DM ${stats.dmCount}개 | Notion 페이지 ${stats.notionPages}개 | DB 변경 ${stats.notionDbs}개`;

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
            text: analysis.slice(0, 3000), // Slack 블록 제한
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
              text: `🕐 생성: ${new Date().toLocaleString('ko-KR')} | 🤖 AI: Claude Sonnet 4`,
            },
          ],
        },
      ],
    });

    // 분석이 길 경우 추가 메시지
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
  // 쿼리 파라미터로 분석 기간 설정 (기본: 1일, 최대: 30일)
  const days = Math.min(parseInt(req.query?.days) || 1, 30);
  const isInitialRun = days > 1;

  console.log('='.repeat(50));
  console.log(`${isInitialRun ? '🚀 초기 분석' : '📅 정기 분석'} 시작: ${new Date().toISOString()}`);
  console.log(`📆 분석 기간: ${days}일`);
  console.log('='.repeat(50));

  try {
    // 1. Slack 메시지 수집
    console.log('\n📱 Slack 채널 메시지 수집 중...');
    const { messages: slackMessages, userMap } = await getSlackMessages(days);
    console.log(`✅ Slack 채널 메시지: ${slackMessages.length}개`);

    // 2. CEO DM 수집
    console.log('\n💬 CEO DM 수집 중...');
    const ceoDMs = await getCEODirectMessages(userMap, days);
    console.log(`✅ CEO DM 메시지: ${ceoDMs.length}개`);

    // 3. Notion 사용자 목록 가져오기
    console.log('\n👥 Notion 사용자 목록 가져오는 중...');
    const notionUsers = await getNotionUsers();
    console.log(`✅ Notion 사용자: ${Object.keys(notionUsers).length}명`);

    // 3. Notion 페이지 수집
    console.log('\n📝 Notion 페이지 수집 중...');
    const notionPages = await getRecentNotionPages();
    console.log(`✅ 업데이트된 페이지: ${notionPages.length}개`);

    // 4. Notion 데이터베이스 수집
    console.log('\n📊 Notion 데이터베이스 수집 중...');
    const notionDatabases = await getNotionDatabases();
    console.log(`✅ 활성 데이터베이스: ${notionDatabases.length}개`);

    // 5. Claude 분석
    console.log('\n🤖 Claude 분석 중...');
    const analysis = await analyzeWithClaude(slackMessages, ceoDMs, {
      pages: notionPages,
      databases: notionDatabases,
      users: notionUsers,
    }, days);
    console.log('✅ 분석 완료');

    // 6. CEO에게 발송
    console.log('\n📤 CEO에게 DM 발송 중...');
    await sendDMToCEO(analysis, {
      slackCount: slackMessages.length,
      dmCount: ceoDMs.length,
      notionPages: notionPages.length,
      notionDbs: notionDatabases.length,
      days: days,
    });

    console.log('\n' + '='.repeat(50));
    console.log('✅ 크론 작업 완료');
    console.log('='.repeat(50));

    res.status(200).json({
      success: true,
      type: days > 1 ? 'initial_analysis' : 'daily_analysis',
      days: days,
      stats: {
        slackMessages: slackMessages.length,
        ceoDMs: ceoDMs.length,
        notionPages: notionPages.length,
        notionDatabases: notionDatabases.length,
      },
      timestamp: new Date().toISOString(),
    });
  } catch (error) {
    console.error('❌ 크론 작업 실패:', error);
    res.status(500).json({
      success: false,
      error: error.message,
      timestamp: new Date().toISOString(),
    });
  }
};
