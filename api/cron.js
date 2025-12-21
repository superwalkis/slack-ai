// ============================================
// 파일 3: api/cron.js
// ============================================
const Anthropic = require('@anthropic-ai/sdk');
const { WebClient } = require('@slack/web-api');

const anthropic = new Anthropic({
  apiKey: process.env.ANTHROPIC_API_KEY,
});

const slack = new WebClient(process.env.SLACK_BOT_TOKEN);

async function getSlackMessages() {
  try {
    const now = Math.floor(Date.now() / 1000);
    const yesterday = now - 86400;

    const channelsResult = await slack.conversations.list({
      types: 'public_channel,private_channel',
      exclude_archived: true,
    });

    let allMessages = [];

    for (const channel of channelsResult.channels) {
      try {
        const history = await slack.conversations.history({
          channel: channel.id,
          oldest: yesterday,
          latest: now,
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

    const usersResult = await slack.users.list();
    const userMap = {};
    usersResult.members.forEach(user => {
      userMap[user.id] = user.real_name || user.name;
    });

    allMessages = allMessages.map(msg => ({
      ...msg,
      userName: userMap[msg.user] || '알 수 없음',
    }));

    return allMessages;
  } catch (error) {
    console.error('Slack 메시지 가져오기 실패:', error);
    return [];
  }
}

async function analyzeWithClaude(messages) {
  if (messages.length === 0) {
    return '어제 Slack에 메시지가 없었습니다.';
  }

  const messageText = messages
    .map(m => `[${m.channel}] ${m.userName}: ${m.text}`)
    .join('\n');

  const prompt = `당신은 CEO의 Staff로서 조직을 모니터링합니다.

어제 Slack 대화 내역:
${messageText}

다음 형식으로 분석해주세요:

📌 긴급 이슈 (우선순위 Top 3)
🔴 [팀명] 이슈 제목
   - 상황: 간단 요약
   - 영향: 비즈니스 임팩트
   - 추천 액션: CEO가 할 일

🟡 주의 필요
   (같은 형식)

🟢 칭찬할 점
   - 팀원 이름
   - 기여 내용
   - 추천 액션

⚠️ 패턴 감지
   - 반복되는 문제
   - 소통 단절 징후
   - 방향성 혼란

분석 시 주의사항:
- 비즈니스 임팩트가 큰 것 우선
- 감정 아닌 사실 기반
- 구체적 액션 아이템
- SuperWalk/DeFi/베이직 모드 관련 특히 주의`;

  try {
    const message = await anthropic.messages.create({
      model: 'claude-sonnet-4-20250514',
      max_tokens: 2000,
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

async function sendDMToCEO(analysis) {
  try {
    await slack.chat.postMessage({
      channel: process.env.CEO_SLACK_ID,
      text: `📊 어제의 조직 모니터링 리포트\n\n${analysis}`,
      blocks: [
        {
          type: 'header',
          text: {
            type: 'plain_text',
            text: '📊 어제의 조직 모니터링 리포트',
          },
        },
        {
          type: 'section',
          text: {
            type: 'mrkdwn',
            text: analysis,
          },
        },
        {
          type: 'context',
          elements: [
            {
              type: 'mrkdwn',
              text: `생성 시간: ${new Date().toLocaleString('ko-KR')} | AI: Claude Sonnet 4`,
            },
          ],
        },
      ],
    });
    console.log('CEO에게 DM 발송 완료');
  } catch (error) {
    console.error('DM 발송 실패:', error);
  }
}

module.exports = async (req, res) => {
  console.log('크론 작업 시작:', new Date().toISOString());

  const messages = await getSlackMessages();
  console.log(`수집된 메시지: ${messages.length}개`);

  const analysis = await analyzeWithClaude(messages);

  await sendDMToCEO(analysis);

  res.status(200).json({
    success: true,
    messagesAnalyzed: messages.length,
    timestamp: new Date().toISOString(),
  });
};
