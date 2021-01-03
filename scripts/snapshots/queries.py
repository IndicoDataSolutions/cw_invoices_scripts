CREATE_TEACH_TASK = """
        mutation createCrowdlabelQuestionnaire($dataType: DataType!, $datasetId: Int!, $name: String!, $numLabelersRequired: Int!, $questions: [QuestionInput]!, $sourceColumnId: Int!, $processors: [InputProcessor]) {
          createQuestionnaire(dataType: $dataType, datasetId: $datasetId, name: $name, numLabelersRequired: $numLabelersRequired, questions: $questions, sourceColumnId: $sourceColumnId, processors: $processors) {
            id
            __typename
          }
        }
        """


GET_TEACH_TASK = """
        query getCrowdlabelQuestionnaire($id: Int!) {
          questionnaires(questionnaireIds: [$id]) {
            questionnaires {
              id
              odl
              name
              questionsStatus
              createdAt
              updatedAt
              active
              datasetId
              subsetId
              sourceColumnId
              instructions
              numTotalExamples
              numFullyLabeled
              numLabeledByMe
              role
              assignedUsers {
                id
                userId
                name
                email
                role
                labelCount
                __typename
              }
              questions {
                id
                targets
                text
                modelGroupId
                labelset {
                  id
                  name
                  numLabelersRequired
                  __typename
                }
                __typename
              }
              __typename
            }
            __typename
          }
        }  
    """

SUBMIT_QUESTIONNAIRE_EXAMPLE = """
        mutation submitQuestionnaireExample($labelsetId: Int!, $datasetId: Int!, $labels: [SubmissionLabel]!, $modelGroupId: Int) {
          submitLabels(datasetId: $datasetId, labels: $labels, labelsetId: $labelsetId, modelGroupId: $modelGroupId) {
            success
            __typename
          }
        }
    """