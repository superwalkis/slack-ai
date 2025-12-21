const Anthropic = require('@anthropic-ai/sdk');
const { WebClient } = require('@slack/web-api');

const anthropic = new Anthropic({
  apiKey: process.env.ANTHROPIC_API_KEY,
});

const slack = new WebClient(process.env.SLACK_BOT_TOKEN);

async function getSlackMessages(days = 7) {
  try {
    const now = Math.floor(Date.now() / 1000);
    const startTime = now - (86400 * days); // days일 전

    const channelsResult = await slack.conversations.list({
      types: 'public_channel,private_channel',
      exclude_archived: true,
    });

    let allMessages = [];
    let channelStats = {};
    let dailyStats = {};
    let userActivity = {};

    // 날짜별 통계 초기화
    for (let i = 0; i < days; i++) {
      const date = new Date(now * 1000 - i * 86400000).toISOString().split('T')[0];
      dailyStats[date] = { messages: 0, channels: new Set() };
    }

    for (const channel of channelsResult.channels) {
      try {
        const history = await slack.conversations.history({
          channel: channel.id,
          oldest: startTime,
          latest: now,
          limit: 1000, // 최대 1000개
        });

        channelStats[channel.name] = {
          messageCount: history.messages.length,
          participants: new Set(),
          lastActive: null,
        };

        history.messages.forEach(msg => {
          // 날짜별 통계
          const msgDate = new Date(parseFloat(msg.ts) * 1000).toISOString().split('T')[0];
          if (dailyStats[msgDate]) {
            dailyStats[msgDate].messages++;
            dailyStats[msgDate].channels.add(channel.name);
          }

          // 사용자 활동
          if (msg.user) {
            channelStats[channel.name].participants.add(msg.user);
            if (!userActivity[msg.user]) {
              userActivity[msg.user] = { messages: 0, channels: new Set() };
            }
            userActivity[msg.user].messages++;
            userActivity[msg.user].channels.add(channel.name);
          }

          // 마지막 활동 시간
          if (!channelStats[channel.name].lastActive || parseFloat(msg.ts) > channelStats[channel.name].lastActive) {
            channelStats[channel.name].lastActive = parseFloat(msg.ts);
          }
        });

        const messagesWithContext = history.messages.map(msg => ({
          channel: channel.name,
          user: msg.user,
          text: msg.text || '',
          timestamp: msg.ts,
          thread_ts: msg.thread_ts,
          reactions: msg.reactions || [],
        }));

        allMessages = allMessages.concat(messagesWithContext);
      } catch (err) {
        console.log(`채널 ${channel.name} 접근 불가:`, err.message);
      }
    }

    // 참여자 수를 숫자로 변환
    Object.keys(channelStats).forEach(ch => {
      channelStats[ch].participants = channelStats[ch].participants.size;
    });

    // 날짜별 채널 수 변환
    Object.keys(dailyStats).forEach(date => {
      dailyStats[date].channels = dailyStats[date].channels.size;
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

    return { messages: allMessages, channelStats, dailyStats, userActivity, userMap };
  } catch (error) {
    console.error('Slack 메시지 가져오기 실패:', error);
    return { messages: [], channelStats: {}, dailyStats: {}, userActivity: {}, userMap: {} };
  }
}

async function analyzeWithClaude(data) {
  const { messages, channelStats, dailyStats, userActivity, userMap } = data;

  if (messages.length === 0) {
    return `📊 최근 7일 Slack 활동 요약

🔇 **최근 7일간 모니터링 중인 채널에 메시지가 없습니다.**

⚠️ **이것은 심각한 신호일 수 있습니다:**
- 봇이 채널에 초대되지 않았거나
- 모든 채널 접근 권한이 없거나
- 실제로 팀 활동이 완전히 중단됐거나

💡 **즉시 확인할 것:**
1. Slack에서 \`/invite @AI Monitor\` 로 봇을 주요 채널에 초대했는지
2. 봇의 채널 접근 권한 확인
3. 팀원들이 다른 도구로 이동했는지

설정이 완료되면 다시 테스트해주세요!`;
  }

  // 날짜별 트렌드 텍스트
  let trendText = '\n📈 일별 활동 추이:\n';
  Object.entries(dailyStats)
    .sort((a, b) => b[0].localeCompare(a[0]))
    .forEach(([date, stats]) => {
      trendText += `${date}: ${stats.messages}개 메시지, ${stats.channels}개 활성 채널\n`;
    });

  // 채널별 통계
  let channelText = '\n📊 채널별 활동 (Top 10):\n';
  Object.entries(channelStats)
    .sort((a, b) => b[1].messageCount - a[1].messageCount)
    .slice(0, 10)
    .forEach(([channel, stats]) => {
      const lastActive = stats.lastActive 
        ? new Date(stats.lastActive * 1000).toLocaleDateString('ko-KR')
        : '알 수 없음';
      channelText += `#${channel}: ${stats.messageCount}개 메시지, ${stats.participants}명 참여, 마지막: ${lastActive}\n`;
    });

  // 활발한 사용자 Top 5
  let userText = '\n👥 가장 활발한 사용자 (Top 5):\n';
  Object.entries(userActivity)
    .sort((a, b) => b[1].messages - a[1].messages)
    .slice(0, 5)
    .forEach(([userId, stats]) => {
      const userName = userMap[userId] || '알 수 없음';
      userText += `${userName}: ${stats.messages}개 메시지, ${stats.channels.size}개 채널\n`;
    });

  // 최근 메시지 샘플 (최근 50개)
  const recentMessages = messages
    .sort((a, b) => parseFloat(b.timestamp) - parseFloat(a.timestamp))
    .slice(0, 50)
    .map(m => {
      const date = new Date(parseFloat(m.timestamp) * 1000).toLocaleDateString('ko-KR');
      return `[${date}] #${m.channel} - ${m.userName}: ${m.text.substring(0, 150)}`;
    })
    .join('\n');

  const prompt = `당신은 CEO의 Staff입니다. 최근 7일간 Slack 대화를 분석하여 CEO가 알아야 할 핵심 내용을 요약해주세요.

# 데이터 요약
- 총 메시지: ${messages.length}개
- 분석 기간: 최근 7일
- 활성 채널: ${Object.keys(channelStats).length}개
${trendText}
${channelText}
${userText}

# 주요 대화 샘플 (최근 50개)
${recentMessages}

# 분석 형식

🔥 **가장 중요한 이슈 Top 3**
1. [채널] 이슈: 간단 요약
   - 왜 중요: 비즈니스 임팩트
   - 추천 액션: 구체적으로

⚠️ **주의 필요한 패턴**
- 반복되는 문제나 병목
- 소통 단절 징후
- 결정이 지연되는 이슈

✅ **잘 진행되는 것**
- 누가/무엇을 잘하고 있는지
- 칭찬할 포인트

📊 **조직 건강도 분석**
- 활동 트렌드 (증가/감소/유지)
- 채널별 생산성
- 팀 사기 신호

🎯 **이번 주 우선순위 액션**
1. 
2. 
3. 

💡 **CEO 인사이트**
- 놓치기 쉬운 중요한 시그널
- 조직 문화/분위기 변화
- 전략적 시사점

---
**분석 원칙:**
- 데이터 기반, 구체적 사실
- 비즈니스 임팩트 중심
- 실행 가능한 조언만
- SuperWalk, DeFi, 베이직 모드, 교보 협업 관련 특히 주의
- 긴급도 높은 것부터`;

  try {
    const message = await anthropic.messages.create({
      model: 'claude-sonnet-4-20250514',
      max_tokens: 4000,
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
    return `⚠️ AI 분석 중 오류 발생

📊 수집된 데이터:
- 메시지: ${messages.length}개
- 채널: ${Object.keys(channelStats).length}개
- 기간: 최근 7일

${channelText}

에러: ${error.message}

데이터는 정상 수집되었으나 AI 분석 과정에서 문제가 발생했습니다.`;
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
            text: '📊 최근 7일 조직 모니터링 리포트',
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
              text: `생성: ${new Date().toLocaleString('ko-KR', {timeZone: 'Asia/Seoul'})} | 분석 기간: 최근 7일 | AI: Claude Sonnet 4`,
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
    const data = await getSlackMessages(7); // 7일간 데이터
    console.log(`수집된 메시지: ${data.messages.length}개`);

    const analysis = await analyzeWithClaude(data);

    await sendDMToCEO(analysis);

    res.status(200).json({
      success: true,
      messagesAnalyzed: data.messages.length,
      channelsMonitored: Object.keys(data.channelStats).length,
      period: '7 days',
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
