const Anthropic = require('@anthropic-ai/sdk');
const { WebClient } = require('@slack/web-api');

const anthropic = new Anthropic({
  apiKey: process.env.ANTHROPIC_API_KEY,
});

const slack = new WebClient(process.env.SLACK_BOT_TOKEN);

async function getSlackMessages() {
  try {
    const now = Math.floor(Date.now() / 1000);
    const yesterday = now - 86400; // 24시간 전

    const channelsResult = await slack.conversations.list({
      types: 'public_channel,private_channel',
      exclude_archived: true,
    });

    let allMessages = [];
    let channelStats = {};

    for (const channel of channelsResult.channels) {
      try {
        const history = await slack.conversations.history({
          channel: channel.id,
          oldest: yesterday,
          latest: now,
        });

        channelStats[channel.name] = {
          messageCount: history.messages.length,
          participants: new Set(),
        };

        const messagesWithContext = history.messages.map(msg => {
          if (msg.user) {
            channelStats[channel.name].participants.add(msg.user);
          }
          return {
            channel: channel.name,
            user: msg.user,
            text: msg.text || '',
            timestamp: msg.ts,
            thread_ts: msg.thread_ts,
            reactions: msg.reactions || [],
          };
        });

        allMessages = allMessages.concat(messagesWithContext);
      } catch (err) {
        console.log(`채널 ${channel.name} 접근 불가:`, err.message);
      }
    }

    // 참여자 수를 숫자로 변환
    Object.keys(channelStats).forEach(ch => {
      channelStats[ch].participants = channelStats[ch].participants.size;
    });

    const usersResult = await slack.users.list();
    const userMap = {};
    usersResult.members.forEach(user => {
      userMap[user.id] = user.real_name || user.name;
    });

    allMessages = allMessages.map(msg => ({
      ...msg,
      userName: userMap[msg.user] || '알 수 없음',
    }));

    return { messages: allMessages, channelStats, userMap };
  } catch (error) {
    console.error('Slack 메시지 가져오기 실패:', error);
    return { messages: [], channelStats: {}, userMap: {} };
  }
}

async function analyzeWithClaude(data) {
  const { messages, channelStats } = data;

  if (messages.length === 0) {
    return `📊 어제 Slack 활동 요약

🔇 어제는 모니터링 중인 채널에 메시지가 없었습니다.

💡 **추천 액션:**
- 팀 활동이 줄어든 건지 확인
- 주말이나 휴일인지 체크
- 중요한 논의가 DM으로 넘어간 건 아닌지 점검

내일 다시 확인하겠습니다! 👋`;
  }

  // 채널별 통계 텍스트 생성
  let statsText = '\n📊 채널별 활동:\n';
  Object.entries(channelStats)
    .sort((a, b) => b[1].messageCount - a[1].messageCount)
    .slice(0, 10)
    .forEach(([channel, stats]) => {
      statsText += `#${channel}: ${stats.messageCount}개 메시지, ${stats.participants}명 참여\n`;
    });

  // 메시지 샘플 (최대 100개만)
  const sampleMessages = messages
    .slice(0, 100)
    .map(m => `[#${m.channel}] ${m.userName}: ${m.text.substring(0, 200)}`)
    .join('\n');

  const prompt = `당신은 CEO의 Staff입니다. 어제 Slack 대화를 분석하여 CEO가 알아야 할 핵심 내용을 요약해주세요.

# 데이터
${statsText}

# 주요 대화 샘플
${sampleMessages}

# 분석 형식 (간결하게!)

📌 **긴급 이슈 (즉시 조치 필요)**
- [채널] 이슈 제목: 요약 (1줄)
  → 추천 액션: 구체적으로 (1줄)

⚠️ **주의 필요 (모니터링)**
- [채널] 상황: 요약
  → 왜 주의: 이유

✅ **잘 진행 중 (칭찬/격려)**
- [채널] 누가/무엇을: 간략히
  → 추천: 칭찬 메시지 예시

📊 **패턴 분석**
- 반복되는 이슈나 병목
- 소통 단절 징후
- 생산성 저하 신호

🎯 **오늘의 액션 아이템**
1. 우선순위 1
2. 우선순위 2
3. 우선순위 3

---
**분석 원칙:**
- 비즈니스 임팩트 큰 것만
- 감정 아닌 사실 기반
- 구체적이고 실행 가능한 조언
- 불필요한 세부사항 제거
- SuperWalk/DeFi/베이직 모드 관련 특히 주의
- 메시지가 적으면 간단하게만`;

  try {
    const message = await anthropic.messages.create({
      model: 'claude-sonnet-4-20250514',
      max_tokens: 3000,
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
    return `⚠️ AI 분석 중 오류가 발생했습니다.

어제 메시지: ${messages.length}개
활성 채널: ${Object.keys(channelStats).length}개

원본 데이터는 정상적으로 수집되었으나, 
분석 과정에서 문제가 발생했습니다.

에러: ${error.message}`;
  }
}

async function sendDMToCEO(analysis) {
  try {
    await slack.chat.postMessage({
      channel: process.env.CEO_SLACK_ID,
      text: analysis,
      blocks: [
        {
          type: 'header',
          text: {
            type: 'plain_text',
            text: '📊 어제의 조직 모니터링 리포트',
            emoji: true,
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
          type: 'divider',
        },
        {
          type: 'context',
          elements: [
            {
              type: 'mrkdwn',
              text: `생성: ${new Date().toLocaleString('ko-KR', {timeZone: 'Asia/Seoul'})} | AI: Claude Sonnet 4`,
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

  try {
    const data = await getSlackMessages();
    console.log(`수집된 메시지: ${data.messages.length}개`);

    const analysis = await analyzeWithClaude(data);

    await sendDMToCEO(analysis);

    res.status(200).json({
      success: true,
      messagesAnalyzed: data.messages.length,
      channelsMonitored: Object.keys(data.channelStats).length,
      timestamp: new Date().toISOString(),
    });
  } catch (error) {
    console.error('크론 작업 실패:', error);
    res.status(500).json({
      success: false,
      error: error.message,
    });
  }
};
